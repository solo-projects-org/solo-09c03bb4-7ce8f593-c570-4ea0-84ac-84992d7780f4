"""Tests for batched delivery of retained instrument events.

Covers the durable claim/download/confirm protocol, disconnect-restart
resumption, interleaving with ingestion and write-format switches, failure
atomicity and the "old batch cannot leapfrog" rule.

Run: PYTHONPATH=/workspace:/workspace/.pylibs \
     python3 -m unittest tests.test_delivery -v
"""
import unittest
from unittest import mock

from app.webapp import main as web_main
from app.webapp.delivery import BATCH_SIZE, DeliveryService

from tests.test_integration import PlatformHarness, V1, writer_v2


def ingest(h, n, start=1):
    """Ingest n events and return their event ids in warehouse order."""
    ids = []
    for i in range(start, start + n):
        r = h.web.post("/api/events", json={
            "payload": {"sample_id": f"S-{i:04d}", "project": "P",
                        "measurement": float(i)}})
        assert r.status_code == 200, r.text
        ids.append(r.json()["event_id"])
    return ids


class DeliveryTestBase(unittest.TestCase):
    def setUp(self):
        self.h = PlatformHarness()
        self.web = self.h.web
        web_main.RECONCILE_INTERVAL = 3600
        web_main._bg_stop.clear()
        self._ctx = self.web
        self._ctx.__enter__()

    def tearDown(self):
        self._ctx.__exit__(None, None, None)
        web_main._bg_stop.set()

    def fresh_delivery(self):
        """Simulate a webapp restart: brand-new service over the same DB."""
        return DeliveryService(self.h.web_db_path)


class TestEmptyAndBasicBatches(DeliveryTestBase):
    def test_01_claim_empty_warehouse_creates_no_batch(self):
        r = self.web.post("/api/delivery/claim").json()
        self.assertTrue(r["ok"])
        self.assertTrue(r["empty"])
        self.assertIsNone(r["batch"])
        self.assertEqual(r["progress"]["total_events"], 0)
        self.assertEqual(r["progress"]["next_unconfirmed_seq"], 1)
        # nothing persisted
        self.assertEqual(self.web.get("/api/delivery/progress")
                         .json()["batches"], [])

    def test_02_claim_returns_warehouse_order_with_full_envelope(self):
        ids = ingest(self.h, 3)
        r = self.web.post("/api/delivery/claim").json()
        self.assertTrue(r["created"])
        b = r["batch"]
        self.assertEqual(b["batch_no"], 1)
        self.assertEqual((b["first_event_seq"], b["last_event_seq"]), (1, 3))
        self.assertEqual(b["event_count"], 3)
        self.assertEqual(b["status"], "claimed")
        evs = b["events"]
        self.assertEqual([e["event_id"] for e in evs], ids)  # warehouse order
        self.assertEqual([e["seq"] for e in evs], [1, 2, 3])
        for e in evs:
            self.assertIn("avro_bytes_b64", e)
            self.assertTrue(e["avro_bytes_b64"])
            self.assertEqual(e["writer_version"], 1)
            self.assertEqual(e["schema"]["name"], V1["name"])
            self.assertEqual(e["schema"]["fields"], V1["fields"])

    def test_03_batch_caps_at_20_and_continues_after_confirm(self):
        ingest(self.h, BATCH_SIZE + 5)
        r = self.web.post("/api/delivery/claim").json()
        self.assertEqual(r["batch"]["event_count"], BATCH_SIZE)
        self.assertEqual((r["batch"]["first_event_seq"],
                          r["batch"]["last_event_seq"]), (1, BATCH_SIZE))
        c = self.web.post("/api/delivery/confirm",
                          json={"batch_no": 1}).json()
        self.assertTrue(c["confirmed"])
        self.assertEqual(c["progress"]["confirmed_events"], BATCH_SIZE)

        r2 = self.web.post("/api/delivery/claim").json()
        self.assertTrue(r2["created"])
        self.assertEqual(r2["batch"]["batch_no"], 2)
        self.assertEqual((r2["batch"]["first_event_seq"],
                          r2["batch"]["last_event_seq"]),
                         (BATCH_SIZE + 1, BATCH_SIZE + 5))
        self.assertEqual(r2["batch"]["event_count"], 5)

    def test_04_download_endpoint_matches_frozen_snapshot(self):
        ingest(self.h, 2)
        self.web.post("/api/delivery/claim")
        d = self.web.get("/api/delivery/batches/1/download").json()
        self.assertEqual(d["batch_no"], 1)
        self.assertEqual(d["event_count"], 2)
        self.assertEqual(d["event_range"],
                         {"first_seq": 1, "last_seq": 2})
        self.assertEqual(len(d["events"]), 2)
        # attachment variant serves the same body with a filename header
        r = self.web.get("/api/delivery/batches/1/download?download=1")
        self.assertIn("attachment", r.headers["content-disposition"])
        self.assertEqual(r.json()["events"], d["events"])
        # unknown batch is a structured 404
        self.assertEqual(self.web.get("/api/delivery/batches/99/download")
                         .status_code, 404)


class TestDisconnectAndRestart(DeliveryTestBase):
    def test_05_repeat_claim_is_stable_including_bytes(self):
        ids = ingest(self.h, 4)
        first = self.web.post("/api/delivery/claim").json()["batch"]
        second = self.web.post("/api/delivery/claim").json()
        self.assertFalse(second["created"])
        self.assertEqual(second["batch"], first)

        # A disconnected/reconnected client gets the identical bytes.
        third = self.fresh_delivery().claim()
        self.assertFalse(third["created"])
        self.assertEqual(third["batch"], first)
        self.assertEqual(
            [e["avro_bytes_b64"] for e in third["batch"]["events"]],
            [e["avro_bytes_b64"] for e in first["events"]])
        self.assertEqual([e["event_id"] for e in third["batch"]["events"]], ids)

    def test_06_restart_then_confirm_resumes_progress(self):
        ingest(self.h, 6)
        self.web.post("/api/delivery/claim")
        # Restart with the batch still unconfirmed.
        d2 = self.fresh_delivery()
        view = d2.claim()
        self.assertFalse(view["created"])
        self.assertEqual(view["batch"]["batch_no"], 1)
        out = d2.confirm(1)
        self.assertTrue(out["confirmed"])
        self.assertEqual(out["progress"]["confirmed_events"], 6)
        # after restart the durable progress is visible through the API too
        st = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertIsNone(st["open_batch"])
        self.assertEqual(st["confirmed_events"], 6)
        self.assertEqual(st["next_unconfirmed_seq"], 7)

    def test_07_confirm_unknown_batch_rejected(self):
        ingest(self.h, 1)
        r = self.web.post("/api/delivery/confirm", json={"batch_no": 77})
        self.assertEqual(r.status_code, 409)
        self.assertIn("does not exist", r.json()["error"])

    def test_08_old_batch_cannot_leapfrog_open_batch(self):
        ingest(self.h, BATCH_SIZE + 1)
        self.web.post("/api/delivery/claim")           # batch 1 open
        self.web.post("/api/delivery/confirm",
                      json={"batch_no": 1})            # confirm 1
        self.web.post("/api/delivery/claim")           # batch 2 open
        # Re-confirming the old batch must not skip batch 2.
        r = self.web.post("/api/delivery/confirm", json={"batch_no": 1})
        self.assertEqual(r.status_code, 409)
        self.assertIn("not the current open batch", r.json()["error"])
        st = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(st["open_batch"]["batch_no"], 2)
        self.assertEqual(st["confirmed_events"], BATCH_SIZE)
        # the open batch itself confirms normally and finishes progress
        self.web.post("/api/delivery/confirm", json={"batch_no": 2})
        st = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertIsNone(st["open_batch"])
        self.assertEqual(st["confirmed_events"], BATCH_SIZE + 1)

    def test_09_confirm_is_idempotent_when_still_latest(self):
        ingest(self.h, 2)
        self.web.post("/api/delivery/claim")
        first = self.web.post("/api/delivery/confirm",
                              json={"batch_no": 1}).json()
        second = self.web.post("/api/delivery/confirm",
                               json={"batch_no": 1}).json()
        self.assertTrue(first["confirmed"])
        self.assertTrue(second["idempotent"])
        # no extra batch row created
        batches = self.web.get("/api/delivery/progress").json()["batches"]
        self.assertEqual([b["batch_no"] for b in batches], [1])


class TestInterleaving(DeliveryTestBase):
    def test_10_claimed_batch_immutable_when_new_events_arrive(self):
        ids = ingest(self.h, 3)
        b1 = self.web.post("/api/delivery/claim").json()["batch"]
        # New events land while batch 1 is unconfirmed: they must not enter it.
        new_ids = ingest(self.h, 2, start=4)
        again = self.web.post("/api/delivery/claim").json()
        self.assertFalse(again["created"])
        self.assertEqual(again["batch"], b1)
        self.assertEqual(
            [e["event_id"] for e in again["batch"]["events"]], ids)
        st = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(st["total_events"], 5)
        # confirm batch 1, then the waiting events form batch 2 in order
        self.web.post("/api/delivery/confirm", json={"batch_no": 1})
        b2 = self.web.post("/api/delivery/claim").json()["batch"]
        self.assertEqual([e["event_id"] for e in b2["events"]], new_ids)
        self.assertEqual((b2["first_event_seq"], b2["last_event_seq"]), (4, 5))

    def test_11_writer_switch_interleaved_keeps_snapshot_bytes_and_schemas(self):
        ingest(self.h, 2)  # v1 events
        b1 = self.web.post("/api/delivery/claim").json()["batch"]
        v1_bytes = [e["avro_bytes_b64"] for e in b1["events"]]

        # Switch the write format while the batch is open.
        sw = self.web.post("/api/switches/writer",
                           json={"schema": writer_v2()})
        self.assertEqual(sw.status_code, 200, sw.text)
        # New event encoded with the v2 writer.
        r = self.web.post("/api/events", json={
            "payload": {"sample_id": "S-v2", "project": "P", "measurement": 9.9,
                        "instrument_id": "GC-1", "tags": [1]}}).json()
        self.assertEqual(r["writer_version"], 2)

        # The open batch is byte-identical and still references the v1 schema.
        same = self.web.post("/api/delivery/claim").json()["batch"]
        self.assertEqual([e["avro_bytes_b64"] for e in same["events"]],
                         v1_bytes)
        self.assertTrue(all(e["writer_version"] == 1 for e in same["events"]))
        self.assertEqual(
            [f["name"] for f in same["events"][0]["schema"]["fields"]],
            [f["name"] for f in V1["fields"]])

        self.web.post("/api/delivery/confirm", json={"batch_no": 1})
        b2 = self.web.post("/api/delivery/claim").json()["batch"]
        self.assertEqual(len(b2["events"]), 1)
        ev = b2["events"][0]
        self.assertEqual(ev["event_id"], r["event_id"])
        self.assertEqual(ev["writer_version"], 2)
        schema_fields = [f["name"] for f in ev["schema"]["fields"]]
        self.assertIn("instrument_id", schema_fields)
        self.assertIn("tags", schema_fields)
        # The v2 snapshot actually carries the bytes of the v2 event.
        status = self.web.get("/api/status").json()
        live = next(e for e in status["events"] if e["id"] == r["event_id"])
        self.assertEqual(ev["avro_bytes_b64"], self._blob_of(live["id"]))

    def _blob_of(self, event_id):
        from app.webapp import db as web_db
        from app.db_common import get_conn
        conn = get_conn(self.h.web_db_path)
        try:
            row = web_db.get_event(conn, event_id)
            return row["blob_b64"]
        finally:
            conn.close()


class TestAtomicityAndProgressView(DeliveryTestBase):
    def test_12_failure_during_claim_leaves_no_half_batch(self):
        ingest(self.h, 5)
        svc = self.h.web  # API path; force the snapshot build to fail once
        original = DeliveryService._snapshot_item
        calls = {"n": 0}

        def boom(self, conn, row):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("simulated snapshot failure")
            return original(self, conn, row)

        with mock.patch.object(DeliveryService, "_snapshot_item", boom):
            with self.assertRaises(RuntimeError):
                DeliveryService(self.h.web_db_path).claim()

        # No batch row exists; no event is skipped; a retry claims all five.
        progress = self.web.get("/api/delivery/progress").json()
        self.assertEqual(progress["batches"], [])
        again = self.web.post("/api/delivery/claim").json()
        self.assertTrue(again["created"])
        self.assertEqual((again["batch"]["first_event_seq"],
                          again["batch"]["last_event_seq"]), (1, 5))

    def test_13_progress_only_advances_on_confirm(self):
        ingest(self.h, BATCH_SIZE + BATCH_SIZE + 1)
        p0 = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(p0["confirmed_events"], 0)
        self.assertIsNone(p0["open_batch"])

        self.web.post("/api/delivery/claim")  # batch 1 open, unconfirmed
        p1 = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(p1["confirmed_events"], 0)
        self.assertEqual(p1["delivered_claimed_events"], BATCH_SIZE)
        self.assertEqual(p1["open_batch"]["batch_no"], 1)

        self.web.post("/api/delivery/confirm", json={"batch_no": 1})
        self.web.post("/api/delivery/claim")  # batch 2 open
        p2 = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(p2["confirmed_events"], BATCH_SIZE)
        self.assertEqual(p2["open_batch"]["batch_no"], 2)

        self.web.post("/api/delivery/confirm", json={"batch_no": 2})
        claim3 = self.web.post("/api/delivery/claim").json()  # batch 3
        self.assertTrue(claim3["created"])
        self.assertEqual(claim3["batch"]["event_count"], 1)
        # repeat claim while unconfirmed stays on the same batch
        repeat = self.web.post("/api/delivery/claim").json()
        self.assertFalse(repeat["created"])
        self.assertEqual(repeat["batch"]["batch_no"], 3)
        self.web.post("/api/delivery/confirm", json={"batch_no": 3})
        p4 = self.web.get("/api/delivery/progress").json()["progress"]
        self.assertEqual(p4["confirmed_events"], 2 * BATCH_SIZE + 1)
        self.assertEqual(p4["total_events"], 2 * BATCH_SIZE + 1)
        # nothing left to claim
        empty = self.web.post("/api/delivery/claim").json()
        self.assertTrue(empty["empty"])

    def test_14_status_includes_delivery_block(self):
        ingest(self.h, 1)
        st = self.web.get("/api/status").json()
        self.assertIn("delivery", st)
        self.assertEqual(st["delivery"]["progress"]["total_events"], 1)
        self.assertEqual(st["delivery"]["batches"], [])

    def test_15_bytes_in_json_are_valid_base64_and_decodable(self):
        import base64
        import fastavro
        import io
        ingest(self.h, 2)
        self.web.post("/api/delivery/claim")
        d = self.web.get("/api/delivery/batches/1/download").json()
        for e in d["events"]:
            blob = base64.b64decode(e["avro_bytes_b64"])
            parsed = fastavro.parse_schema(e["schema"])
            datum = fastavro.schemaless_reader(io.BytesIO(blob), parsed)
            self.assertEqual(datum["sample_id"], e["payload"]["sample_id"])
            self.assertEqual(datum["measurement"], e["payload"]["measurement"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
