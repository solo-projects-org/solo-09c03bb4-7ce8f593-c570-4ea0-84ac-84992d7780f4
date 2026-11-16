"""Live HTTP verification of batch delivery across three real processes,
including a webapp restart (disconnect/resume) and interleaving.

Start the services first (fresh data dir), then run:
  PYTHONPATH=/workspace/.pylibs python3 tests/smoke_delivery_live.py
"""
import json
import os
import subprocess
import sys
import time

import httpx

WEB = "http://127.0.0.1:8000"
BASE = os.environ.get("SMOKE_DIR", "/tmp/delivery-smoke")
PYTHON = sys.executable
ENV = dict(os.environ, PYTHONPATH="/workspace:/workspace/.pylibs")

PASS = FAIL = 0
procs = []


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"[PASS] {name}")
    else:
        FAIL += 1
        print(f"[FAIL] {name} :: {detail}")


def start(role, port, extra_env):
    env = dict(ENV, **extra_env)
    log = open(f"{BASE}/{role}.log", "w")
    module = ("app.webapp.main:app" if role == "webapp"
              else "app.analyzer.app:app")
    p = subprocess.Popen(
        [PYTHON, "-m", "uvicorn", module,
         "--host", "127.0.0.1", "--port", str(port)],
        cwd="/workspace", env=env, stdout=log, stderr=subprocess.STDOUT)
    procs.append(p)
    return p


def wait_up(url, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=1).raise_for_status()
            return True
        except Exception:
            time.sleep(0.3)
    return False


def start_webapp():
    return start("webapp", 8000, {
        "WEBAPP_DB": f"{BASE}/webapp.db",
        "ANALYZER_A_URL": "http://127.0.0.1:8001",
        "ANALYZER_B_URL": "http://127.0.0.1:8002",
        "RECONCILE_INTERVAL": "3600",
    })


def main():
    os.makedirs(BASE, exist_ok=True)
    for f in os.listdir(BASE):
        os.remove(os.path.join(BASE, f))

    start("analyzer-a", 8001,
          {"ANALYZER_ID": "analyzer-a", "ANALYZER_DB": f"{BASE}/a.db"})
    start("analyzer-b", 8002,
          {"ANALYZER_ID": "analyzer-b", "ANALYZER_DB": f"{BASE}/b.db"})
    if not wait_up("http://127.0.0.1:8001/health") or \
       not wait_up("http://127.0.0.1:8002/health"):
        return stop("analyzers failed to start")
    wp = start_webapp()
    if not wait_up(WEB + "/api/status"):
        return stop("webapp failed to start")

    web = httpx.Client(base_url=WEB, timeout=15)

    # 1. empty claim
    r = web.post("/api/delivery/claim").json()
    check("empty warehouse -> empty claim, no batch",
          r["empty"] and r["batch"] is None and r["progress"]["total_events"] == 0)

    # 2. ingest 25 events -> claim caps at 20
    for i in range(1, 26):
        web.post("/api/events", json={
            "payload": {"sample_id": f"S-{i}", "project": "P",
                        "measurement": float(i)}})
    c1 = web.post("/api/delivery/claim").json()
    check("first claim creates batch #1 with 20 events in warehouse order",
          c1["created"] and c1["batch"]["batch_no"] == 1
          and c1["batch"]["event_count"] == 20
          and [e["seq"] for e in c1["batch"]["events"]] == list(range(1, 21))
          and c1["batch"]["events"][0]["payload"]["sample_id"] == "S-1")
    check("each event carries bytes, writer version and schema",
          all(e["avro_bytes_b64"] and e["writer_version"] == 1
              and e["schema"]["name"] == "InstrumentEvent"
              for e in c1["batch"]["events"]))

    # 3. download equals the frozen snapshot
    d1 = web.get("/api/delivery/batches/1/download").json()
    check("download JSON matches claimed snapshot",
          d1["events"] == c1["batch"]["events"] and d1["event_count"] == 20)
    hdr = web.get("/api/delivery/batches/1/download?download=1")
    check("attachment download sets Content-Disposition",
          "delivery-batch-1.json" in hdr.headers.get("content-disposition", ""))

    # 4. repeat claim stable
    c1b = web.post("/api/delivery/claim").json()
    check("repeat claim returns identical batch (no new batch)",
          not c1b["created"] and c1b["batch"] == c1["batch"])

    # 5. DISCONNECT: kill the webapp mid-delivery (batch unconfirmed), restart
    wp.terminate()
    wp.wait(timeout=10)
    procs.remove(wp)
    wp = start_webapp()
    if not wait_up(WEB + "/api/status"):
        return stop("webapp restart failed")
    c1c = web.post("/api/delivery/claim").json()
    check("after restart: same batch number/range/bytes resume from unconfirmed",
          not c1c["created"] and c1c["batch"]["batch_no"] == 1
          and c1c["batch"]["first_event_seq"] == 1
          and c1c["batch"]["last_event_seq"] == 20
          and [e["avro_bytes_b64"] for e in c1c["batch"]["events"]]
          == [e["avro_bytes_b64"] for e in c1["batch"]["events"]])

    # 6. interleaving: ingest 3 more events + writer switch while batch open
    for i in range(26, 29):
        web.post("/api/events", json={
            "payload": {"sample_id": f"S-{i}", "project": "P",
                        "measurement": float(i)}})
    st = web.get("/api/status").json()
    v2 = json.loads(json.dumps(st["effective_writer"]["schema"]))
    v2["fields"].append({"name": "instrument_id", "type": "string",
                         "default": "UNKNOWN"})
    v2["fields"].append({"name": "tags",
                         "type": {"type": "array", "items": "int"},
                         "default": []})
    sw = web.post("/api/switches/writer", json={"schema": v2})
    check("writer switch during open batch still confirms",
          sw.status_code == 200 and sw.json()["switch"]["status"] == "confirmed")
    v2ev = web.post("/api/events", json={
        "payload": {"sample_id": "S-29", "project": "P", "measurement": 29.0,
                    "instrument_id": "GC-9", "tags": [7, 8]}}).json()
    check("new event encoded at writer v2", v2ev["writer_version"] == 2)

    c1d = web.post("/api/delivery/claim").json()
    check("open batch unchanged despite new events and format switch",
          c1d["batch"]["batch_no"] == 1 and c1d["batch"]["event_count"] == 20
          and all(e["writer_version"] == 1 for e in c1d["batch"]["events"]))

    # 7. confirm old-batch leapfrog protection: cannot confirm nonexistent #2
    bad = web.post("/api/delivery/confirm", json={"batch_no": 99})
    check("confirm unknown batch -> 409", bad.status_code == 409)

    # 8. confirm batch 1 -> progress 20; batch 2 contains events 21..29 (9)
    conf = web.post("/api/delivery/confirm", json={"batch_no": 1}).json()
    check("confirm batch 1 advances progress to 20",
          conf["confirmed"] and conf["progress"]["confirmed_events"] == 20)
    again = web.post("/api/delivery/confirm", json={"batch_no": 1}).json()
    check("re-confirm old (latest) batch is idempotent",
          again["idempotent"] and again["progress"]["confirmed_events"] == 20)

    c2 = web.post("/api/delivery/claim").json()
    check("batch 2 covers seq 21-29 incl. the v2 event with v2 schema",
          c2["created"] and c2["batch"]["first_event_seq"] == 21
          and c2["batch"]["last_event_seq"] == 29
          and c2["batch"]["event_count"] == 9
          and c2["batch"]["events"][-1]["event_id"] == v2ev["event_id"]
          and c2["batch"]["events"][-1]["writer_version"] == 2
          and "instrument_id" in [f["name"] for f in
                                   c2["batch"]["events"][-1]["schema"]["fields"]])

    # old batch 1 can no longer leapfrog open batch 2
    leap = web.post("/api/delivery/confirm", json={"batch_no": 1})
    check("old batch cannot confirm past open batch 2", leap.status_code == 409)
    web.post("/api/delivery/confirm", json={"batch_no": 2})
    empty = web.post("/api/delivery/claim").json()
    check("all delivered -> empty claim", empty["empty"])
    prog = web.get("/api/delivery/progress").json()["progress"]
    check("final progress: 29 events confirmed, no open batch",
          prog["confirmed_events"] == 29 and prog["total_events"] == 29
          and prog["open_batch"] is None)

    # 9. static page renders the new delivery section
    page = web.get("/").text
    check("web UI contains delivery section",
          "分批交付" in page and "btn-delivery-claim" in page)

    return stop(None)


def stop(err):
    for p in procs:
        p.terminate()
    for p in procs:
        try:
            p.wait(timeout=10)
        except Exception:
            p.kill()
    if err:
        print("SETUP ERROR:", err)
        return 1
    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
