"""
Thin wrapper around kafka-python's producer.

Two things the handout specifically calls out and this module exists to
satisfy:
  - "Kafka needs a few seconds to elect itself leader even as a single
    broker. Your producer and consumer must retry on connection-refused,
    not crash." (Part 2.5) -> see connect_with_retry().
  - "/health must report anything other than ok until both Kafka and
    Neo4j are genuinely reachable -- not merely 'the container has
    started'." (Part 4) -> is_connected() does a real broker metadata
    call, not just "did __init__ not throw".
"""
from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger("kafka_client")

TOPIC = "csv-rows"


class KafkaUnavailableError(RuntimeError):
    pass


class KafkaClient:
    def __init__(self, bootstrap_servers: str, topic: str = TOPIC):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer = None

    def connect_with_retry(self, max_attempts: int = 30, delay_seconds: float = 2.0) -> None:
        """Retry loop for the 'broker hasn't elected a leader yet' window.
        Called once at service startup; /health reflects the outcome via
        is_connected() afterwards rather than caching this result forever,
        so a broker that later drops is honestly reported too."""
        from kafka import KafkaProducer  # local import: keeps this module
        from kafka.errors import NoBrokersAvailable  # importable without kafka installed for pure unit tests

        last_err = None
        for attempt in range(1, max_attempts + 1):
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=5,
                )
                logger.info("Connected to Kafka on attempt %d", attempt)
                return
            except NoBrokersAvailable as exc:
                last_err = exc
                logger.warning(
                    "Kafka not reachable yet (attempt %d/%d): %s",
                    attempt, max_attempts, exc,
                )
                time.sleep(delay_seconds)
        raise KafkaUnavailableError(f"Kafka unreachable after {max_attempts} attempts") from last_err

    def is_connected(self) -> bool:
        if self._producer is None:
            return False
        try:
            # bootstrap_connected() reflects live broker connectivity, not
            # just "constructor didn't throw at startup".
            return bool(self._producer.bootstrap_connected())
        except Exception:
            return False

    def publish_row(self, dataset_id: str, job_id: str, row_index: int,
                     content_hash: str, values: dict) -> None:
        if self._producer is None:
            raise KafkaUnavailableError("publish_row called before connect_with_retry()")
        message = {
            "dataset_id": dataset_id,
            "job_id": job_id,
            "row_index": row_index,
            "content_hash": content_hash,
            "values": values,
        }
        # Key by content_hash (not row_index) so identical rows partition
        # together -- a minor extra safety net on top of the MERGE itself.
        self._producer.send(self.topic, key=content_hash, value=message)

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()
