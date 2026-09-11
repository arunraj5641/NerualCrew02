"""
Thin wrapper around the official neo4j driver.

Design decision (see REPORT.md 'How api knows kafka/neo4j are ready' row):
instead of treating "Bolt handshake succeeded" as ready, is_connected()
performs a tiny canary write + read + delete in a single transaction. The
handout warns "Neo4j accepts the Bolt port later than the container
reports 'started'" (6.3) -- what it doesn't spell out is that Bolt can
also accept connections *before* the database is actually willing to
service a write (e.g. mid-recovery), so a plain `RETURN 1` ping can still
report false-positive readiness in that narrow window. A real write/read
round trip is the stricter, more honest check the handout is really
asking for.
"""
from __future__ import annotations

import logging

logger = logging.getLogger("neo4j_client")


class Neo4jUnavailableError(RuntimeError):
    pass


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str, database: str = "neo4j"):
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self._driver = None

    def connect(self) -> None:
        from neo4j import GraphDatabase
        self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def is_connected(self) -> bool:
        """Canary write-then-read-then-delete. Returns False on *any*
        failure -- including 'driver never connected' -- rather than
        raising, because /health needs a boolean it can always report."""
        if self._driver is None:
            return False
        try:
            with self._driver.session(database=self.database) as session:
                session.execute_write(self._canary_write)
                ok = session.execute_read(self._canary_read)
                session.execute_write(self._canary_cleanup)
                return ok
        except Exception as exc:  # noqa: BLE001 - health check must never throw
            logger.warning("Neo4j canary check failed: %s", exc)
            return False

    @staticmethod
    def _canary_write(tx):
        tx.run("MERGE (c:_HealthCanary {id: 1}) SET c.ts = timestamp()")

    @staticmethod
    def _canary_read(tx):
        record = tx.run("MATCH (c:_HealthCanary {id: 1}) RETURN c.ts AS ts").single()
        return record is not None

    @staticmethod
    def _canary_cleanup(tx):
        tx.run("MATCH (c:_HealthCanary {id: 1}) DELETE c")

    def ensure_dataset_node(self, dataset_id: str, filename: str, rows_total: int) -> None:
        with self._driver.session(database=self.database) as session:
            session.execute_write(self._merge_dataset, dataset_id, filename, rows_total)

    @staticmethod
    def _merge_dataset(tx, dataset_id, filename, rows_total):
        tx.run(
            """
            MERGE (d:Dataset {id: $dataset_id})
            ON CREATE SET d.filename = $filename,
                          d.uploaded_at = timestamp(),
                          d.rows_total = $rows_total
            """,
            dataset_id=dataset_id, filename=filename, rows_total=rows_total,
        )

    def merge_row(self, dataset_id: str, content_hash: str, row_index: int, values: dict) -> None:
        """The single most load-bearing method in this project: MERGE
        (never CREATE), keyed on content_hash. Running this twice for the
        same row is guaranteed to leave the graph in exactly the same
        state -- see csv_utils.row_content_hash for why content_hash and
        not row_index is the key."""
        with self._driver.session(database=self.database) as session:
            session.execute_write(self._merge_row_tx, dataset_id, content_hash, row_index, values)

    @staticmethod
    def _merge_row_tx(tx, dataset_id, content_hash, row_index, values):
        tx.run(
            """
            MATCH (d:Dataset {id: $dataset_id})
            MERGE (r:Row {content_hash: $content_hash})
            SET r.dataset_id = $dataset_id,
                r.row_index = $row_index,
                r += $values
            MERGE (d)-[:HAS_ROW]->(r)
            """,
            dataset_id=dataset_id, content_hash=content_hash,
            row_index=row_index, values=values,
        )

    def run_cypher(self, cypher: str, params: dict | None = None) -> list:
        with self._driver.session(database=self.database) as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]

    def close(self) -> None:
        if self._driver is not None:
            self._driver.close()
