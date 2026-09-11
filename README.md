# CSV → Kafka → Neo4j Chatbot

Built against the "IT HAPPENS @ RA ALE #2 / RISE @ RST #5" hackathon
handout. Full requirements checklist and honest results are in
[`REPORT.md`](./REPORT.md) — read that first for what actually works and
what's caveated.

## What's real vs. substituted

This was built in a sandboxed environment **without a Docker daemon and
without network access to Docker Hub / package registries for Kafka or
Neo4j images**. Everything below is real, working, tested code:

- All application logic (`api/`, `loader/`, `ui/`) — genuinely correct
  Python/JS, not pseudocode.
- The full test suite (`tests/`, 29 tests) — **actually run, all
  passing**, against the real `csv_utils`, `chatbot`, and FastAPI route
  logic, using in-memory fakes in place of a live Kafka broker and Neo4j
  instance.
- `docker-compose.yml` and both `Dockerfile`s — written correctly to the
  handout's spec (pinned image tags, non-root users, healthchecks,
  `condition: service_healthy` ordering) but **not validated against a
  live `docker compose up`** in this environment. See `REPORT.md` §9.6
  for the two specific things to double check before trusting this at a
  real event (Kafka KRaft env-var compatibility, `cypher-shell` path in
  the healthcheck).

If you have Docker available, `docker compose up --build` from this
directory should bring up the real stack — that's the very next thing
to verify.

## Layout

```
docker-compose.yml   # all 5 services, healthchecks, service_healthy ordering
.env.example          # NEO4J_USER / NEO4J_PASSWORD / NEO4J_DATABASE
api/                   # FastAPI: /ingest /status /chat /health
loader/                # Kafka consumer -> Neo4j MERGE
ui/                     # single-page upload/preview/chat UI (nginx)
data/                   # sample_clean.csv, sample_large.csv (5,000 rows),
                        # sample_broken.csv, empty.csv, header_only.csv,
                        # not_a_csv.png -- the hostile-input fixtures
tests/                  # 29 tests, real logic + fake Kafka/Neo4j
scripts/smoke_test.py   # real end-to-end transcript used in REPORT.md §9.4
REPORT.md               # the handout's required report, all 7 sections
```

## Quick start

See `REPORT.md` §9.7 for full run instructions, including the
reproducibility check and how to run the test suite.
