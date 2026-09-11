"""
Runs the real api app in-process (FastAPI TestClient) with fake
Kafka/Neo4j standing in for real infra (no Docker in this environment --
see REPORT.md). Uploads the real sample_clean.csv, simulates the loader
committing every row (so /status and /chat have real data to answer
from), then asks 8 real questions and prints the real, executed
answer/cypher/grounded for each -- this is the actual data used to fill
in REPORT.md section 9.4, not fabricated.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "tests"))

from fastapi.testclient import TestClient
import app.main as main_module
from test_api import FakeKafka, FakeNeo4j  # reuse the same fakes as the test suite

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

fake_kafka = FakeKafka(connected=True)
fake_neo4j = FakeNeo4j(connected=True)
main_module.kafka_client = fake_kafka
main_module.neo4j_client = fake_neo4j
main_module.JOBS.clear()

with TestClient(main_module.app) as client:
    with open(os.path.join(DATA_DIR, "sample_clean.csv"), "rb") as f:
        res = client.post("/ingest", files={"file": ("sample_clean.csv", f, "text/csv")})
    ingest_body = res.json()
    print("=== /ingest ===")
    print(json.dumps(ingest_body, indent=2))
    job_id = ingest_body["job_id"]
    dataset_id = main_module.JOBS[job_id].dataset_id

    # Simulate the loader: MERGE each row's real content into the fake
    # graph (mirrors loader.py's merge_row_tx exactly, minus the real
    # Bolt round trip), then ack /status per row -- exactly the sequence
    # loader.py performs against a real Neo4j instance.
    import csv as csv_mod
    with open(os.path.join(DATA_DIR, "sample_clean.csv"), newline="") as f:
        reader = csv_mod.DictReader(f)
        fake_neo4j.rows[dataset_id] = []
        for row in reader:
            fake_neo4j.rows[dataset_id].append(row)
            client.post("/status/_ack", json={"job_id": job_id, "succeeded": True})

    status_body = client.get("/status", params={"job_id": job_id}).json()
    print("\n=== /status (after simulated load) ===")
    print(json.dumps(status_body, indent=2))

    health_body = client.get("/health").json()
    print("\n=== /health ===")
    print(json.dumps(health_body, indent=2))

    questions = [
        "How many rows are there?",
        "How many rows belong to the Billing group?",
        "How many rows belong to the Support group?",
        "What are the distinct values of group?",
        "What columns does this data have?",
        "How many rows have status = 'Approved'?",       # 'status' column doesn't exist
        "What is the average amount?",                     # not implemented -> should be honest
        "How many rows belong to the Nonexistent group?",  # real column, fake value
    ]

    print("\n=== /chat transcript (8 real questions) ===")
    rows_for_report = []
    for q in questions:
        res = client.post("/chat", json={"question": q})
        body = res.json()
        print(f"\nQ: {q}")
        print(f"A: {body['answer']}")
        print(f"cypher: {body['cypher']}")
        print(f"result: {body['result']}")
        print(f"grounded: {body['grounded']}")
        rows_for_report.append((q, body["answer"], body["grounded"]))

    print("\n=== Markdown table for REPORT.md 9.4 ===")
    print("| Question asked | Answer given | Correct? | Grounded? |")
    print("|---|---|---|---|")
    for q, a, g in rows_for_report:
        print(f"| {q} | {a} | (see notes) | {g} |")
