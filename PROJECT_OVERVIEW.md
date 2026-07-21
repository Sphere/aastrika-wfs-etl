# Aastrika Telemetry ETL Service — Project Overview

## What it does

A batch ETL pipeline that reads raw learning-platform telemetry events out of **Elasticsearch**, computes **session/content time-spent summaries**, and writes the results into **PostgreSQL**. Event names (`START`/`END`/`INTERACT`/`IMPRESSION`, `pdata`, `rollup.l1`) are typical of a Sunbird/DIKSHA-style learning telemetry system.

## Architecture

```
__main__.py → TelemetryOrchestrator → ElasticsearchExtractor → TelemetryAggregator → PostgresLoader
```

| Layer | File | Responsibility |
|---|---|---|
| Config | `src/aastrika_telemetry/config/settings.py` | Loads all config from `.env` via Pydantic (`AppConfig`) |
| Config | `src/aastrika_telemetry/config/es_config.py` | Singleton Elasticsearch client |
| Config | `src/aastrika_telemetry/config/postgres_config.py` | Postgres connection pool (`SimpleConnectionPool`) |
| Models | `src/aastrika_telemetry/models/events.py` | `TelemetryEvent` + nested `Actor`, `Context`, `EventObject`, `EventData` — mirrors raw ES event shape |
| Models | `src/aastrika_telemetry/models/summary.py` | `TelemetrySummary` — one row per (session, content, course, user) group |
| Extract | `src/aastrika_telemetry/extractors/es_extractor.py` | Builds ES query, scrolls through matching events |
| Transform | `src/aastrika_telemetry/transformers/telemetry_aggregator.py` | **Core business calculation** (see below) |
| Load | `src/aastrika_telemetry/loaders/postgres_loader.py` | Creates table/indexes if missing, batch-inserts summaries |
| Orchestration | `src/aastrika_telemetry/pipeline/orchestrator.py` | Wires the three stages together |

## Step 1 — Extract (`es_extractor.py`)

- Filters ES for 4 event types: `START`, `END`, `INTERACT`, `IMPRESSION`.
- Time window: intended to use `HOURS_WINDOW` from `.env`, but a hardcoded override (lines 51–52, marked `#TODO: testing purpose only, remove later`) currently always fetches the **last 100 hours to now**, regardless of config. This is live in the pipeline, not just in tests.
- Uses the ES `scroll` API (2-minute scroll context) to page through all matching docs, converting each into a `TelemetryEvent`.
- `parse_es_event` unwraps a nested `telemetry.events` structure (or falls back to flat) and extracts `actor`, `context`, `object`, `edata`.

## Step 2 — Transform: the core calculation (`telemetry_aggregator.py`)

This is where "time spent on content within a session" is computed.

1. **Grouping** — events are bucketed by composite key `(session_id, content_id, course_id, user_id)`.
   - `content_id` = `object.id`
   - `course_id` = `object.rollup.l1`
   - Events missing `sid`, `actor.id`, `content_id`, or `course_id` are dropped.
2. **Per-group processing** (sorted by timestamp `ets`):
   - `start_ets` = timestamp of the **first** `START` event. If no `START` exists, the **entire group is discarded**.
   - `end_ets` = timestamp of the **last** `END` event occurring at or after `start_ets`. If none exists, falls back to the **last event in the group** (any type) and flags `end_imputed=True`.
   - **Duration** = `end_ets - start_ets`. Note: the variable is named `duration_sec` but no division by 1000 occurs — since `ets` is in milliseconds, the value stored is actually milliseconds despite the name.
   - If duration ≤ 0, the record is logged as an error and **discarded** (tracked via `negative_duration_count`).
3. **Unique ID**: `mid = SESCNT_{DDMMYYYY}_{session_id}_{content_id}_{course_id}` — used as the Postgres `ON CONFLICT` idempotency key. Built from **today's date**, not the event's date.
4. `platform_id` (`context.pdata.id`) and `channel_id` (`context.channel`) are captured from the **START event only**.
5. Logs summary counts: negative-duration records skipped, total groups processed.

**Not yet implemented:**
- `event_env` ("Home"/"Learn"/"Profile") — defined in the model but never populated; always `None`.
- `device_id` — always `None`.
- `total_interactions` — commented out in the model ("TODO: uncomment when interaction data is available"); `INTERACT`/`IMPRESSION` events are fetched from ES but currently unused in aggregation. Only `START`/`END` drive the calculation today.

## Step 3 — Load (`postgres_loader.py`)

- Auto-creates the `telemetry_summary` table (name from `POSTGRES_TABLE` env var) plus indexes on `content_id`, `user_id`, `session_id`, `course_id` if the table doesn't exist.
- Batch inserts via `execute_batch`, with `mid` as a `UNIQUE` constraint and `ON CONFLICT (mid) DO NOTHING` for idempotency (subject to the `mid` date caveat above).

## Entry points

- `main.py` — dev-only connection smoke test (ES `ping()` and Postgres `SELECT 1`).
- `python -m aastrika_telemetry` — runs the real pipeline via `TelemetryOrchestrator.run_pipeline()`. Note: the connection test call here is commented out (`__main__.py` lines 54–57), so the pipeline runs directly without a pre-flight check.

## Known rough edges / TODOs

1. **Hardcoded 100-hour window** in `build_query` overrides the configured `HOURS_WINDOW` — marked temporary but currently active.
2. **`mid` uses today's date, not the event's date** — re-running the pipeline later for the same historical session/content won't dedupe against previously-loaded rows, risking duplicates.
3. `ElasticsearchConfig.close_client()` / `PostgresConfig.close_pool()` are defined but never called anywhere — no cleanup on pipeline exit (acknowledged via TODO comments).
4. Interaction/impression events are extracted but not used in aggregation yet.
5. `event_env` and `device_id` are always null — populate-from-event logic is missing.
6. No real unit/integration tests yet — `tests/` only contains empty `__init__.py` files.
7. Naming: `duration_sec` actually holds a millisecond delta, not seconds, which could confuse downstream consumers of `total_time_duration`.

## Configuration reference (`.env`)

| Variable | Purpose |
|---|---|
| `ES_HOST`, `ES_PORT`, `ES_SCHEME`, `ES_USERNAME`, `ES_PASSWORD` | Elasticsearch connection |
| `ES_INDEX` / `ES_INDEX_PATTERN` + `EX_INDEX_PATTERN_SET` | Which ES index to query (static name vs. date-pattern) |
| `ES_TIMEOUT`, `ES_MAX_RETRIES` | ES client resilience settings |
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_SCHEMA` | PostgreSQL connection |
| `POSTGRES_POOL_SIZE`, `POSTGRES_MAX_OVERFLOW` | Connection pool sizing |
| `POSTGRES_TABLE` | Target table name for summaries |
| `BATCH_SIZE` | ES scroll page size / Postgres batch insert size |
| `MAX_WORKERS`, `RETRY_ATTEMPTS`, `RETRY_DELAY` | Reserved for future use (not currently referenced in code) |
| `HOURS_WINDOW` | Intended extraction time window (currently overridden — see rough edges) |
