"""Batch delivery of retained instrument events to the analysis group.

The warehouse operator hands retained events out in *durable, confirmable
batches*. Delivery state lives entirely in SQLite so a disconnected client
(or a webapp restart) always resumes from the unconfirmed batch:

* ``claim`` - atomically freeze the next up-to-20 events into a new batch.
  Only one batch may be open (``claimed``) at a time. Re-claiming while a
  batch is still unconfirmed returns the *same* batch with an identical
  batch number, event range and Avro bytes, even across a restart.
* ``download`` - JSON of the frozen snapshot (never recomputed from live
  tables), each event carrying its id, original Avro bytes (Base64), the
  writer version in force when it was ingested and the matching schema.
* ``confirm`` - the only operation that moves delivery progress forward.
  Confirming an older batch while a newer one is still open is rejected, so
  re-confirming an old batch can never skip past unconfirmed events.

Interleaving guarantees
-----------------------
A claimed batch stores its own snapshot; events ingested afterwards (and any
write-format switch) never mutate it - new events only enter *later* batches.
Claim and confirm each run inside one SQLite transaction (``BEGIN
IMMEDIATE``) plus a process lock, so a failure can never leave a half-written
batch or skip an unconfirmed event. With no new events a claim returns an
empty result and creates no batch.
"""
from __future__ import annotations

import json
import threading
from typing import Any, Dict, List, Optional

from . import db

BATCH_SIZE = 20


class DeliveryError(Exception):
    """A delivery precondition failed (unknown / non-open batch, etc.)."""


class DeliveryService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Mirrors SwitchService's process-level serialization; SQLite's
        # BEGIN IMMEDIATE additionally serializes across connections.
        self._lock = threading.RLock()

    def _conn(self):
        from ..db_common import get_conn
        return get_conn(self.db_path)

    # ======================================================================
    # Claim / confirm
    # ======================================================================

    def claim(self) -> Dict[str, Any]:
        """Return the open batch, or atomically freeze a new one.

        Returns ``{"created": False, "empty": True, ...progress}`` when no
        unclaimed event exists and no batch is currently open.
        """
        with self._lock:
            conn = self._conn()
            try:
                # Take the write lock up-front: concurrent claims serialize,
                # and a crash anywhere rolls the whole batch back.
                conn.execute("BEGIN IMMEDIATE")
                open_row = db.latest_delivery_batch(conn)
                if open_row is not None and open_row["status"] == "claimed":
                    # Unconfirmed batch: repeat claim is a stable re-delivery.
                    progress = self._progress_locked(conn)
                    return {"created": False, "empty": False,
                            "batch": self._batch_view(open_row),
                            "progress": progress}

                total = db.count_events(conn)
                if open_row is None:
                    next_seq = 1
                else:
                    next_seq = open_row["last_event_seq"] + 1
                if next_seq > total:
                    conn.commit()
                    return {"created": False, "empty": True, "batch": None,
                            "progress": self._progress_locked(conn)}

                last_seq = min(next_seq + BATCH_SIZE - 1, total)
                rows = db.events_in_range(conn, next_seq, last_seq)
                # Defensive: the range must be contiguous and complete,
                # otherwise the transaction rolls back and no batch exists.
                if len(rows) != last_seq - next_seq + 1:
                    raise DeliveryError(
                        "warehouse event range is not contiguous; refusing to "
                        "create a partial batch")
                snapshot = [self._snapshot_item(conn, r) for r in rows]
                batch_no = db.insert_delivery_batch(
                    conn, first_event_seq=next_seq, last_event_seq=last_seq,
                    event_count=len(snapshot), snapshot=snapshot)
                conn.commit()
                row = db.get_delivery_batch(conn, batch_no)
                return {"created": True, "empty": False,
                        "batch": self._batch_view(row),
                        "progress": self._progress_locked(conn)}
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def confirm(self, batch_no: int) -> Dict[str, Any]:
        """Advance progress by confirming exactly the current open batch."""
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = db.get_delivery_batch(conn, batch_no)
                if row is None:
                    conn.rollback()
                    raise DeliveryError(f"batch {batch_no} does not exist")
                latest = db.latest_delivery_batch(conn)
                if latest["batch_no"] != batch_no:
                    # An older batch must never leapfrog the open one.
                    conn.rollback()
                    raise DeliveryError(
                        f"batch {batch_no} is not the current open batch "
                        f"(current: {latest['batch_no']}); confirm batches in order")
                if row["status"] == "confirmed":
                    # Idempotent: an already-confirmed latest batch answers
                    # success without moving any pointer.
                    conn.commit()
                    return {"confirmed": False, "idempotent": True,
                            "batch": self._batch_view(row),
                            "progress": self._progress_locked(conn)}
                db.confirm_delivery_batch(conn, batch_no)
                conn.commit()
                row = db.get_delivery_batch(conn, batch_no)
                return {"confirmed": True, "idempotent": False,
                        "batch": self._batch_view(row),
                        "progress": self._progress_locked(conn)}
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    # ======================================================================
    # Views / downloads
    # ======================================================================

    def get_batch(self, batch_no: int) -> Dict[str, Any]:
        with self._lock:
            conn = self._conn()
            try:
                row = db.get_delivery_batch(conn, batch_no)
                if row is None:
                    raise DeliveryError(f"batch {batch_no} does not exist")
                return self._batch_view(row)
            finally:
                conn.close()

    def download_batch(self, batch_no: int) -> Dict[str, Any]:
        """Full frozen JSON envelope; bytes come from the stored snapshot."""
        batch = self.get_batch(batch_no)
        return {
            "batch_no": batch["batch_no"],
            "status": batch["status"],
            "event_range": {"first_seq": batch["first_event_seq"],
                            "last_seq": batch["last_event_seq"]},
            "event_count": batch["event_count"],
            "created_at": batch["created_at"],
            "confirmed_at": batch["confirmed_at"],
            "events": batch["events"],
        }

    def progress(self) -> Dict[str, Any]:
        with self._lock:
            conn = self._conn()
            try:
                return self._progress_locked(conn)
            finally:
                conn.close()

    def list_batches(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._conn()
            try:
                return [self._batch_meta(r)
                        for r in db.list_delivery_batches(conn, limit)]
            finally:
                conn.close()

    # ======================================================================
    # Internal helpers
    # ======================================================================

    def _snapshot_item(self, conn, row) -> Dict[str, Any]:
        """Freeze one event exactly as retained: bytes + writer schema."""
        schema = db.get_schema_by_fp(conn, row["writer_fp"])
        return {
            "seq": row["seq"],
            "event_id": row["id"],
            "writer_version": row["writer_version"],
            "writer_fp": row["writer_fp"],
            "avro_bytes_b64": row["blob_b64"],
            "schema": schema,
            "payload": json.loads(row["payload_json"]),
            "ingested_at": row["ingested_at"],
        }

    @staticmethod
    def _batch_view(row) -> Dict[str, Any]:
        view = DeliveryService._batch_meta(row)
        view["events"] = json.loads(row["snapshot_json"])
        return view

    @staticmethod
    def _batch_meta(row) -> Dict[str, Any]:
        return {
            "batch_no": row["batch_no"],
            "status": row["status"],
            "first_event_seq": row["first_event_seq"],
            "last_event_seq": row["last_event_seq"],
            "event_count": row["event_count"],
            "created_at": row["created_at"],
            "confirmed_at": row["confirmed_at"],
        }

    def _progress_locked(self, conn) -> Dict[str, Any]:
        total_events = db.count_events(conn)
        latest = db.latest_delivery_batch(conn)
        if latest is None:
            confirmed_through = 0
            open_batch = None
        else:
            if latest["status"] == "confirmed":
                confirmed_through = latest["last_event_seq"]
                open_batch = None
            else:
                # Everything before the open batch is confirmed (batches are
                # strictly sequential), the open range itself is not.
                confirmed_through = latest["first_event_seq"] - 1
                open_batch = self._batch_meta(latest)
        return {
            "total_events": total_events,
            "confirmed_events": min(confirmed_through, total_events),
            "delivered_claimed_events": (latest["last_event_seq"]
                                         if latest is not None else 0),
            "next_unconfirmed_seq": confirmed_through + 1,
            "open_batch": open_batch,
            "batch_size": BATCH_SIZE,
        }
