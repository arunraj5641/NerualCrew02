# CSV → Kafka → Neo4j Chatbot

Built against the "IT HAPPENS @ RA ALE #2 / RISE @ RST #5" hackathon
handout. Full requirements checklist, live verification results, and honest limitations are in
[`REPORT.md`](./REPORT.md).

## What is Live and Verified

The complete 5-tier architecture has been verified end-to-end under live Docker Compose execution:

- **Full Application Pipeline**: CSV upload → FastAPI (`POST /ingest` returning HTTP 202) → Kafka topic `csv-rows` → streaming Loader → Neo4j graph `MERGE` → Grounded Chatbot (`POST /chat`).
- **Full Test Suite**: All 29 unit and integration tests pass cleanly (`29/29 passing`).
- **Live Docker Compose Stack**: All 5 containers (`kafka`, `neo4j`, `api`, `loader`, `ui`) run healthy and properly coordinated via `service_healthy` ordering.
- **Strict Non-Root Security**: Containers run unprivileged (`appuser` for API & Loader, `nginx` (uid 101) for UI via `libcap` port 80 binding).
- **True Idempotency**: Re-uploading identical CSV data produces zero duplicate `Row` nodes or `HAS_ROW` relationships in Neo4j.
- **Structural Grounding**: Questions referencing non-existent columns or out-of-scope queries return `grounded: false` with `"I don't have that in the data."`.
- **Modern React UI**: Migrated to React + Vite, served via production non-root Nginx on `http://localhost:3000`, featuring drag & drop file upload, live progress bar, health indicator pill, and an interactive chat with an expandable Evidence drawer (grounded state, Cypher query, raw results).

## Layout

```
docker-compose.yml   # All 5 services, healthchecks, service_healthy ordering
.env.example          # NEO4J_USER / NEO4J_PASSWORD / NEO4J_DATABASE
api/                   # FastAPI service: /ingest (202), /status, /chat, /health
loader/                # Kafka consumer -> Neo4j idempotent MERGE + /status/_ack
ui/                     # React + Vite frontend, multi-stage non-root Nginx container
data/                   # sample_clean.csv, sample_large.csv (5,000 rows),
                        # sample_broken.csv, empty.csv, header_only.csv, not_a_csv.png
tests/                  # 29 tests, 100% passing
scripts/smoke_test.py   # Real end-to-end transcript used in REPORT.md §9.4
REPORT.md               # Complete verified report with all 7 sections
```

## Quick Start

```bash
# 1. Prepare environment variables
cp .env.example .env

# 2. Start the full stack
docker compose up -d

# 3. Verify health
curl http://localhost:8000/health
# Returns {"status":"ok","kafka_connected":true,"neo4j_connected":true}

# 4. Open the React UI
# Access http://localhost:3000 in your browser

# 5. Run test suite
python3 -m pytest tests/ -v
```
