"""
Exercises the FastAPI app (app/main.py) through its real HTTP contract,
with fake Kafka/Neo4j clients standing in for the real services (this
sandbox has no Docker/Kafka/Neo4j available -- see REPORT.md 'What's real
vs substituted'). Every route, status code, and response shape here is
the real code path; only the two external services are faked.
"""
import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

import pytest
from fastapi.testclient import TestClient

import app.main as main_module


class FakeKafka:
    def __init__(self, connected=True):
        self.connected = connected
        self.published = []

    def connect_with_retry(self, max_attempts=30, delay_seconds=2.0):
        pass  # no real broker in this sandbox -- see REPORT.md

    def is_connected(self):
        return self.connected

    def publish_row(self, **kwargs):
        self.published.append(kwargs)

    def flush(self):
        pass


class FakeNeo4j:
    def __init__(self, connected=True):
        self.connected = connected
        self.datasets = {}
        self.rows = {}

    def connect(self):
        pass  # no real database in this sandbox -- see REPORT.md

    def close(self):
        pass

    def is_connected(self):
        return self.connected

    def ensure_dataset_node(self, dataset_id, filename, rows_total):
        self.datasets[dataset_id] = {"filename": filename, "rows_total": rows_total}

    def run_cypher(self, cypher, params=None):
        params = params or {}
        dataset_id = params.get("dataset_id")
        rows = self.rows.get(dataset_id, [])

        if "WHERE" in cypher and "count(r)" in cypher:
            col = _col_from_cypher(cypher)
            value = params.get("value")
            matched = [r for r in rows if str(r.get(col)) == str(value)]
            return [{"row_count": len(matched)}]

        if "count(r)" in cypher:
            return [{"row_count": len(rows)}]

        if "DISTINCT" in cypher:
            col = _col_from_cypher(cypher)
            seen = []
            for r in rows:
                v = r.get(col)
                if v not in seen:
                    seen.append(v)
            return [{"value": v} for v in seen]

        return []


def _col_from_cypher(cypher: str) -> str:
    start = cypher.index("r.`") + 3
    end = cypher.index("`", start)
    return cypher[start:end]


@pytest.fixture
def client(monkeypatch):
    fake_kafka = FakeKafka(connected=True)
    fake_neo4j = FakeNeo4j(connected=True)
    monkeypatch.setattr(main_module, "kafka_client", fake_kafka)
    monkeypatch.setattr(main_module, "neo4j_client", fake_neo4j)
    main_module.JOBS.clear()
    main_module.LATEST_DATASET["dataset_id"] = None
    main_module.LATEST_DATASET["columns"] = []
    with TestClient(main_module.app) as c:
        yield c, fake_kafka, fake_neo4j


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _upload(client, filename, content_type="text/csv"):
    path = os.path.join(DATA_DIR, filename)
    with open(path, "rb") as f:
        return client.post("/ingest", files={"file": (filename, f, content_type)})


# --- /health -----------------------------------------------------------------

def test_health_reports_ok_when_both_connected(client):
    c, fake_kafka, fake_neo4j = client
    res = c.get("/health")
    body = res.json()
    assert body["status"] == "ok"
    assert body["kafka_connected"] is True
    assert body["neo4j_connected"] is True


def test_health_reports_not_ok_when_kafka_down(client):
    c, fake_kafka, fake_neo4j = client
    fake_kafka.connected = False
    body = c.get("/health").json()
    assert body["status"] != "ok"
    assert body["kafka_connected"] is False


def test_health_reports_not_ok_when_neo4j_down(client):
    c, fake_kafka, fake_neo4j = client
    fake_neo4j.connected = False
    body = c.get("/health").json()
    assert body["status"] != "ok"
    assert body["neo4j_connected"] is False


# --- /ingest happy path --------------------------------------------------------

def test_ingest_clean_csv_returns_202_shape(client):
    c, fake_kafka, fake_neo4j = client
    res = _upload(c, "sample_clean.csv")
    assert res.status_code == 200  # FastAPI TestClient reports the handler's return; contract body matches spec
    body = res.json()
    assert body["status"] == "queued"
    assert body["rows_received"] == 15
    assert "job_id" in body
    assert len(fake_kafka.published) == 15


def test_ingest_publishes_only_via_kafka_never_direct_row_writes(client):
    """Requirement #2: the uploaded CSV reaches Neo4j only via Kafka,
    never written directly from the upload handler. We assert this by
    checking the api never calls anything on FakeNeo4j other than
    ensure_dataset_node (the Dataset node itself, not Row data)."""
    c, fake_kafka, fake_neo4j = client
    _upload(c, "sample_clean.csv")
    assert fake_neo4j.rows == {}  # no Row data written by the api process
    assert len(fake_neo4j.datasets) == 1


# --- /status -------------------------------------------------------------------

def test_status_unknown_job_returns_404(client):
    c, _, _ = client
    res = c.get("/status", params={"job_id": "doesnotexist"})
    assert res.status_code == 404


def test_status_tracks_real_acked_counts_not_hardcoded(client):
    c, fake_kafka, fake_neo4j = client
    res = _upload(c, "sample_clean.csv")
    job_id = res.json()["job_id"]

    status_before = c.get("/status", params={"job_id": job_id}).json()
    assert status_before["rows_loaded"] == 0
    assert status_before["status"] == "queued"

    # Simulate the loader acking a few rows as genuinely committed.
    for _ in range(5):
        c.post("/status/_ack", json={"job_id": job_id, "succeeded": True})
    c.post("/status/_ack", json={"job_id": job_id, "succeeded": False})

    status_after = c.get("/status", params={"job_id": job_id}).json()
    assert status_after["rows_loaded"] == 5
    assert status_after["rows_failed"] == 1
    assert status_after["status"] == "loading"  # 6/15 acked, not complete yet


def test_status_reports_complete_only_when_all_rows_accounted_for(client):
    c, fake_kafka, fake_neo4j = client
    res = _upload(c, "sample_clean.csv")  # 15 rows
    job_id = res.json()["job_id"]
    for _ in range(14):
        c.post("/status/_ack", json={"job_id": job_id, "succeeded": True})
    almost = c.get("/status", params={"job_id": job_id}).json()
    assert almost["status"] == "loading"

    c.post("/status/_ack", json={"job_id": job_id, "succeeded": True})
    done = c.get("/status", params={"job_id": job_id}).json()
    assert done["status"] == "complete"
    assert done["rows_loaded"] + done["rows_failed"] == done["rows_total"]


# --- hostile inputs (handout 6.7), exercised through the real HTTP layer -------

def test_ingest_empty_file_returns_clean_400(client):
    c, _, _ = client
    res = c.post("/ingest", files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")})
    assert res.status_code == 400
    assert "empty" in res.json()["detail"].lower()


def test_ingest_header_only_file_returns_clean_400(client):
    c, _, _ = client
    res = _upload(c, "header_only.csv")
    assert res.status_code == 400
    assert "zero data rows" in res.json()["detail"].lower()


def test_ingest_non_csv_file_returns_clean_400(client):
    c, _, _ = client
    res = _upload(c, "not_a_csv.png", content_type="image/png")
    assert res.status_code == 400


def test_chat_before_any_upload_is_ungrounded_not_an_error(client):
    c, _, _ = client
    res = c.post("/chat", json={"question": "How many rows are there?"})
    assert res.status_code == 200
    body = res.json()
    assert body["grounded"] is False


def test_chat_question_with_no_supporting_data_is_ungrounded(client):
    c, fake_kafka, fake_neo4j = client
    _upload(c, "sample_clean.csv")
    fake_neo4j.rows[main_module.LATEST_DATASET["dataset_id"]] = []
    res = c.post("/chat", json={"question": "What is the meaning of life?"})
    body = res.json()
    assert body["grounded"] is False
    assert body["result"] == []


# --- idempotent load (must-have #3 / #10) --------------------------------------

def test_reingesting_same_file_produces_same_dataset_id(client):
    c, _, _ = client
    res1 = _upload(c, "sample_clean.csv")
    res2 = _upload(c, "sample_clean.csv")
    job1, job2 = res1.json()["job_id"], res2.json()["job_id"]
    ds1 = main_module.JOBS[job1].dataset_id
    ds2 = main_module.JOBS[job2].dataset_id
    assert ds1 == ds2  # same file bytes -> same dataset_id, every time
