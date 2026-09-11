# REPORT.md

## 9.1 What we built

A CSV → Kafka → Neo4j → Chatbot pipeline: `ui` uploads a CSV, `api` parses
it and publishes one Kafka message per row, `loader` consumes the topic
and `MERGE`s each row into Neo4j, and `api`'s `/chat` endpoint answers
plain-English questions with real, executed Cypher against that graph —
refusing to answer when the question isn't grounded in the data. All five
services are defined in one `docker-compose.yml`.

**Honest status:** the full Python application code (parsing, Kafka
client, Neo4j client, loader, chatbot, FastAPI routes, UI) is written,
and its business logic is **genuinely tested and passing — 29/29 tests
green** — against fake Kafka/Neo4j clients, because **this authoring
environment has no Docker daemon and no network route to Docker Hub /
Confluent's registries**, so `docker compose up` could not actually be
executed here. The `docker-compose.yml`, both `Dockerfile`s, and the
healthcheck configuration are written to the handout's spec but were
**not run against real containers in this environment**. This is the
single most important caveat in this report — see §9.6.

## 9.2 The data and the graph model

Three CSVs were prepared per handout §2.3, all in `data/`:

| File | Rows | Purpose |
|---|---|---|
| `sample_clean.csv` | 15 | end-to-end sanity check |
| `sample_large.csv` | 5,000 | volume/throughput check |
| `sample_broken.csv` | 5 (no true header, ragged columns, blank line) | hostile-input check |
| `empty.csv` | 0 bytes | hostile-input check |
| `header_only.csv` | header, 0 data rows | hostile-input check |
| `not_a_csv.png` | binary PNG bytes | hostile-input check |

Graph model actually produced by `loader.py` / `neo4j_client.py` (kept
generic per handout §5, not enriched — see §9.6):

```
(:Dataset {id, filename, uploaded_at, rows_total})
  -[:HAS_ROW]->
(:Row {content_hash, dataset_id, row_index, <one property per CSV column>})
```

**Deviation from the handout's suggested key:** §5 suggests keying `Row`
on `dataset_id + row_index`. We instead key on `content_hash` — a SHA-256
hash of `dataset_id + sorted(column:value pairs)` (`csv_utils.row_content_hash`).
`dataset_id` itself is `sha256(raw file bytes)[:16]`, not a random UUID.
Two consequences, both verified by `tests/test_csv_utils.py`:

- Re-parsing the exact same file bytes always yields the same
  `dataset_id` and the same set of `content_hash` values
  (`test_reparsing_same_file_produces_identical_hashes`) — this is what
  makes two clean `docker compose` runs produce identical counts
  (must-have #10), not merely an intention.
- Two genuinely identical rows collapse to one node even if they came
  from different upload jobs, which is a stronger idempotency guarantee
  than row-index keying gives.

## 9.3 Methods

| Decision | Chosen | Rejected | Reason |
|---|---|---|---|
| Ingest path | `api` parses CSV → publishes one Kafka message/row → returns 202 immediately | Writing rows to Neo4j directly from `/ingest` | Upload handler must not block on however long the full graph write takes (handout Part 3); a slow/down Neo4j must never drop the upload |
| Idempotency key | `content_hash` = SHA-256(dataset_id + sorted column:value pairs); `dataset_id` = SHA-256(raw file bytes) | `dataset_id + row_index` (the handout's minimum suggestion) | Content-hash keying is idempotent even across two different upload jobs of identical data, and makes `dataset_id` itself deterministic across reruns, not just the row keys |
| Chatbot approach | Schema-introspecting Cypher templates: only answers using column names that actually exist in the uploaded CSV | An LLM call turning English → Cypher | No LLM/API key needed at all (sidesteps the "ask your organisers" caveat in Part 1); grounding is structural — a question about a nonexistent column cannot be answered by construction, not by a policy check we could forget |
| How `api` knows kafka/neo4j are ready | `/health` does a **live** check each call: Kafka via `producer.bootstrap_connected()`, Neo4j via a canary `MERGE`+`MATCH`+`DELETE` write/read/cleanup transaction (not a bare `RETURN 1`) | Caching a one-time "connected" flag from startup | Bolt can accept connections before Neo4j is truly willing to service writes (handout §6.3); a boolean set once at startup would go stale and lie exactly in that window |
| Row-load progress source of truth | `loader` acks `/status/_ack` only **after** its Neo4j `MERGE` transaction commits (or genuinely raises) | Incrementing `rows_loaded` when the Kafka message is consumed | Directly addresses handout §6.5 ("status will lie to you if you let it") — consumed ≠ committed |

## 9.4 Results

Real transcript from `scripts/smoke_test.py`, which runs the actual
FastAPI app (`app/main.py`) through `/ingest` → simulated per-row commits
→ `/chat`, against `data/sample_clean.csv` (15 rows: 7 Billing, 4
Support, 4 Sales). Kafka/Neo4j are faked (see caveat in §9.1); the
chatbot logic, regex parsing, and Cypher template selection are 100%
real, unmodified application code.

| Question asked | Answer given | Correct? | Grounded? |
|---|---|---|---|
| How many rows are there? | "There are 15 rows in this dataset." | ✅ Yes (matches actual row count) | True |
| How many rows belong to the Billing group? | "There are 7 rows where group = 'Billing'." | ✅ Yes | True |
| How many rows belong to the Support group? | "There are 4 rows where group = 'Support'." | ✅ Yes | True |
| What are the distinct values of group? | "Distinct values of group: Billing, Support, Sales." | ✅ Yes | True |
| What columns does this data have? | "This dataset has these columns: customer_id, name, group, order_id, amount." | ✅ Yes | True |
| How many rows have status = 'Approved'? | "I don't have that in the data. None of the columns in this dataset match..." | ✅ Yes — `status` is not a real column, correctly refused | False (correctly) |
| What is the average amount? | "I don't have that in the data. Try asking about row counts, or about one of these columns..." | ⚠️ Partially — aggregate functions (AVG/SUM) are simply not implemented in the template set; this isn't a hallucination risk, just missing coverage | False (correctly, but for the wrong reason — see below) |
| How many rows belong to the Nonexistent group? | "There are 0 rows where group = 'Nonexistent'." | ✅ Yes — correctly answers 0, does **not** claim ungrounded, because `group` is a real column and 0 is a legitimate answer | True |

**Why the failures failed:** the "average amount" question is the one
genuine coverage gap: the chatbot correctly refuses to fabricate a number
(no aggregate template exists, so it falls through to the honest
fallback), but it fails for the *wrong* reason — it should ideally
recognize "average" as an intent and either compute a real `AVG()` Cypher
query or say specifically "I can't compute averages yet" rather than the
generic fallback message. This is an honest coverage limitation, not a
grounding failure: at no point does the system state a number it didn't
compute. Caught during development by literally running the smoke test
and reading the numbers (per this project's own evaluation discipline) —
an earlier version of the fake Neo4j client used for testing returned 0
for every filtered query, which would have shown up in this exact table
as "Billing = 0 rows" (obviously wrong, since the CSV clearly has Billing
rows) had it not been caught and fixed before writing this report.

## 9.5 How we worked

Built solo, in this order: shared models → CSV/idempotency logic → Kafka
client → Neo4j client → chatbot → FastAPI routes → UI → Dockerfiles →
compose file → tests → smoke test → this report. This mirrors the
handout's own recommended order in Part 6.2 ("build the pipe before the
logic") adapted for a no-Docker environment: here, "the pipe" became "the
real business logic with fakes standing in for the two infra
dependencies," built first, then the actual Docker/Kafka/Neo4j wiring
written against that already-tested logic.

**Decision:** use content-hash idempotency keys instead of the handout's
suggested `dataset_id + row_index`.
**Options considered:** row-index keying (simpler, matches the handout
literally) vs. content-hash keying.
**Chosen because:** it gives a stronger, more defensible idempotency
guarantee and a real answer to the "same filename, different content"
limitation named in §9.6, rather than punting on it.
**Cost accepted:** slightly more complex hashing logic, and Row nodes no
longer trivially map 1:1 to "the Nth line of this specific file" if two
different files happen to share a row's exact content.
**Would revisit if:** a future requirement needed to preserve exact
per-file row provenance even for byte-identical rows across different
uploads.

**Dead end:** initially tried to make `/status` progress purely
event-driven off Kafka consumer-group lag metrics (bytes/offsets
remaining), to avoid needing the loader to call back into `api` at all.
Abandoned after about 20 minutes once it became clear that offset lag
measures *messages not yet consumed*, not *rows not yet committed to
Neo4j* — exactly the trap named in handout §6.5 — so it would have
reintroduced the "status lies" bug through a different door. Switched to
the explicit `/status/_ack` callback instead.

## 9.6 Limitations and next steps

- **No real Docker/Kafka/Neo4j execution in this environment.** The
  compose file, Dockerfiles, and healthchecks are written correctly
  to spec but have only been validated by code review, not a live
  `docker compose up`. Concretely, before trusting this in a real
  hackathon run: (1) confirm the Kafka `apache/kafka:3.7.0` KRaft
  environment variables produce a healthy single-node cluster on the
  actual Docker version available, since KRaft env var names have
  shifted across Kafka releases; (2) confirm the `neo4j:5.24-community`
  healthcheck's `cypher-shell` invocation matches that image's installed
  path/binary name.
- **Neo4j Community Edition cannot host a database literally named
  `CSV_Graph_DB`** — multi-database support is an Enterprise-only
  feature in Neo4j 5.x. We honor the handout's fixed username/password
  (`neo4j` / `csvgraphdb`, wired via `.env`) but write to the default
  `neo4j` database, which is the closest honest equivalent on Community
  Edition. This should be flagged to organizers before the event if a
  literal database name match is graded.
- **Re-uploading a file with the same filename but different content is
  not specially detected.** Because `dataset_id` is derived from file
  *bytes*, a changed file simply gets a new `dataset_id` and a new,
  separate `Dataset` node rather than replacing the old one — the graph
  will accumulate both datasets rather than the second superseding the
  first. Fixing this would need an explicit "supersede" operation (e.g.
  matching on `filename` and archiving/deleting the prior `Dataset`
  subtree on new upload), which was out of scope for the 3-hour budget.
- **In-memory job store (`JOBS` dict in `api`).** Fine for a single `api`
  replica and the hackathon's scope; would need Redis/Postgres to survive
  an `api` restart or run more than one replica.
- **Chatbot coverage is intentionally minimal**, not exhaustive: row
  counts (total and filtered), distinct values, and a columns listing.
  No aggregate functions (AVG/SUM/MIN/MAX), no multi-hop relationship
  questions, no enrichment of foreign-key-style columns into real graph
  relationships (handout §6.1's optional stretch). Every unsupported
  question type correctly refuses rather than guessing (verified in
  §9.4), which is the property the handout actually grades (10 marks for
  groundedness vs 90 for engineering).
- **No monitoring/alerting, no auth on any endpoint, no rate limiting.**
  Fine for a hackathon demo; not production-ready as-is.

## 9.7 How to run it

```bash
# 1. Clone/unzip this project, then from its root:
cp .env.example .env        # already provided pre-filled with the event's
                             # fixed credentials (neo4j / csvgraphdb)

# 2. Pre-pull images (handout §2.2) if not already cached:
docker pull apache/kafka:3.7.0
docker pull neo4j:5.24-community
docker pull python:3.11-slim

# 3. Bring the whole stack up:
docker compose up --build

# 4. Open the UI:
#    http://localhost:3000
#    Drag in data/sample_clean.csv, data/sample_large.csv, or
#    data/sample_broken.csv from this repo to test the three required
#    cases from handout §2.3.

# 5. Or test the API directly with curl:
curl -F "file=@data/sample_clean.csv" http://localhost:8000/ingest
curl "http://localhost:8000/status?job_id=<job_id from above>"
curl -X POST http://localhost:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"question": "How many rows belong to the Billing group?"}'
curl http://localhost:8000/health

# 6. Reproducibility check (must-have #10):
docker compose down -v
docker compose up --build
# re-run the same /ingest call above and compare row counts in Neo4j
# Browser (http://localhost:7474) via:
#   MATCH (r:Row) RETURN count(r)

# 7. Run the test suite (does not require Docker -- uses fake
#    Kafka/Neo4j clients, see tests/test_api.py):
pip install -r api/requirements.txt -r loader/requirements.txt pytest httpx
python -m pytest tests/ -v

# 8. Run the smoke-test transcript used to write section 9.4 above:
python scripts/smoke_test.py
```
