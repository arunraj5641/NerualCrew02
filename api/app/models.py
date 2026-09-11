"""
Shared response/request shapes for the API. Kept dependency-light (plain
dataclasses) so they're trivially importable from tests without needing the
full FastAPI/pydantic stack spun up.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class IngestResponse:
    job_id: str
    rows_received: int
    status: str = "queued"

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "rows_received": self.rows_received,
            "status": self.status,
        }


@dataclass
class StatusResponse:
    job_id: str
    status: str  # queued | loading | complete | failed
    rows_total: int
    rows_loaded: int
    rows_failed: int

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "status": self.status,
            "rows_total": self.rows_total,
            "rows_loaded": self.rows_loaded,
            "rows_failed": self.rows_failed,
        }


@dataclass
class ChatResponse:
    answer: str
    cypher: str
    result: list
    grounded: bool

    def to_dict(self) -> dict:
        return {
            "answer": self.answer,
            "cypher": self.cypher,
            "result": self.result,
            "grounded": self.grounded,
        }


@dataclass
class HealthResponse:
    status: str  # "ok" or anything else
    kafka_connected: bool
    neo4j_connected: bool

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "kafka_connected": self.kafka_connected,
            "neo4j_connected": self.neo4j_connected,
        }


@dataclass
class JobRecord:
    """Server-side bookkeeping for one upload. rows_loaded/rows_failed are
    only ever incremented by the loader after a Neo4j write genuinely
    commits (or genuinely fails) -- never on Kafka consume alone. See
    REPORT.md Methods table, 'How api knows kafka/neo4j are ready' /
    idempotency rows for the reasoning."""

    job_id: str
    dataset_id: str
    filename: str
    rows_total: int
    rows_loaded: int = 0
    rows_failed: int = 0
    status: str = "queued"
    columns: list = field(default_factory=list)

    def refresh_status(self) -> None:
        if self.rows_loaded + self.rows_failed >= self.rows_total and self.rows_total > 0:
            self.status = "complete"
        elif self.rows_loaded > 0 or self.rows_failed > 0:
            self.status = "loading"

    def to_status(self) -> StatusResponse:
        return StatusResponse(
            job_id=self.job_id,
            status=self.status,
            rows_total=self.rows_total,
            rows_loaded=self.rows_loaded,
            rows_failed=self.rows_failed,
        )
