"""
FastAPI service: the 'api' box in the architecture diagram.

  POST /ingest  -> parses the CSV, publishes one Kafka message per row,
                    returns 202 immediately (upload handler never blocks
                    on however long the full write takes -- see handout
                    Part 3, 'Why put Kafka in the middle').
  GET  /status  -> real row counts, sourced from the in-memory JobRecord
                    that the loader updates over HTTP as rows commit.
  POST /chat    -> grounded chatbot, see app/chatbot.py.
  GET  /health  -> not 'ok' until Kafka AND Neo4j are genuinely reachable.

Job state lives in-memory here for hackathon scope (single api replica).
The handout doesn't require multi-replica api, and a Redis/Postgres job
store would be the natural next step -- noted in REPORT.md limitations.
"""
from __future__ import annotations

import os
import uuid
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .csv_utils import (
    parse_csv_bytes, EmptyCSVError, NoDataRowsError, NotACSVError,
)
from .models import JobRecord
from .kafka_client import KafkaClient, KafkaUnavailableError
from .neo4j_client import Neo4jClient
from .chatbot import answer_question

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("api")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD")  # must come from env -- see handout 6.4
NEO4J_DATABASE = os.environ.get("NEO4J_DATABASE", "neo4j")

if not NEO4J_PASSWORD:
    logger.warning(
        "NEO4J_PASSWORD is not set. Set it via docker-compose environment / "
        ".env, never hard-code it (handout 6.4)."
    )

kafka_client = KafkaClient(KAFKA_BOOTSTRAP)
neo4j_client = Neo4jClient(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD or "", NEO4J_DATABASE)

# job_id -> JobRecord. Also keep the most recently completed dataset's
# id/columns so /chat has something to ground against without requiring
# the client to resend job_id on every question.
JOBS: dict[str, JobRecord] = {}
LATEST_DATASET: dict = {"dataset_id": None, "columns": []}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Connect to Kafka (with retry, per handout 2.5) and Neo4j at
    startup. Failure here does not crash the api process -- /health will
    simply and honestly report not-ok until connectivity exists, which is
    what lets the container come up even if Kafka is still electing a
    leader (handout 6.3)."""
    try:
        kafka_client.connect_with_retry(max_attempts=30, delay_seconds=2.0)
    except KafkaUnavailableError:
        logger.error("Kafka not reachable at startup; /health will report not-ok until it is.")
    try:
        neo4j_client.connect()
    except Exception:
        logger.error("Neo4j driver failed to initialize at startup.")
    yield
    kafka_client.flush()
    neo4j_client.close()


app = FastAPI(title="CSV -> Kafka -> Neo4j Chatbot API", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# POST /ingest
# ---------------------------------------------------------------------------
@app.post("/ingest", status_code=202)
async def ingest(file: UploadFile = File(...)):
    raw = await file.read()

    try:
        parsed = parse_csv_bytes(raw, file.filename or "upload.csv")
    except EmptyCSVError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NoDataRowsError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except NotACSVError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    job_id = uuid.uuid4().hex[:8]
    job = JobRecord(
        job_id=job_id,
        dataset_id=parsed.dataset_id,
        filename=parsed.filename,
        rows_total=len(parsed.rows),
        columns=parsed.columns,
    )
    JOBS[job_id] = job

    # Register the Dataset node up front so the loader's row MERGEs have
    # something to attach HAS_ROW to immediately (idempotent: MERGE with
    # ON CREATE SET, see neo4j_client.ensure_dataset_node).
    try:
        neo4j_client.ensure_dataset_node(parsed.dataset_id, parsed.filename, len(parsed.rows))
    except Exception as exc:
        logger.warning("Could not pre-create Dataset node (loader will retry via its own writes): %s", exc)

    published = 0
    try:
        for row in parsed.rows:
            kafka_client.publish_row(
                dataset_id=parsed.dataset_id,
                job_id=job_id,
                row_index=row.row_index,
                content_hash=row.content_hash,
                values=row.values,
            )
            published += 1
        kafka_client.flush()
    except KafkaUnavailableError as exc:
        job.status = "failed"
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")

    job.status = "queued"
    LATEST_DATASET["dataset_id"] = parsed.dataset_id
    LATEST_DATASET["columns"] = parsed.columns

    return {
        "job_id": job_id,
        "rows_received": published,
        "status": "queued",
    }


# ---------------------------------------------------------------------------
# GET /status
# ---------------------------------------------------------------------------
@app.get("/status")
def status(job_id: str):
    job = JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{job_id}'")
    return job.to_status().to_dict()


class RowAckRequest(BaseModel):
    job_id: str
    succeeded: bool = True


@app.post("/status/_ack")
def status_ack(ack: RowAckRequest):
    """Internal endpoint the loader calls once per row, AFTER the Neo4j
    MERGE transaction actually commits (or genuinely fails) -- never on
    Kafka consume alone. This is what makes /status row counts real
    instead of a lie (handout 6.5)."""
    job = JOBS.get(ack.job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Unknown job_id '{ack.job_id}'")
    if ack.succeeded:
        job.rows_loaded += 1
    else:
        job.rows_failed += 1
    job.refresh_status()
    return {"ok": True}


# ---------------------------------------------------------------------------
# POST /chat
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    question: str


@app.post("/chat")
def chat(req: ChatRequest):
    result = answer_question(
        question=req.question,
        neo4j_client=neo4j_client,
        dataset_id=LATEST_DATASET["dataset_id"],
        columns=LATEST_DATASET["columns"],
    )
    return {
        "answer": result.answer,
        "cypher": result.cypher,
        "result": result.result,
        "grounded": result.grounded,
    }


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    k_ok = kafka_client.is_connected()
    n_ok = neo4j_client.is_connected()
    return {
        "status": "ok" if (k_ok and n_ok) else "not_ok",
        "kafka_connected": k_ok,
        "neo4j_connected": n_ok,
    }
