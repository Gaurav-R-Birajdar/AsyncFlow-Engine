# Changelog

All notable changes to AsyncFlow Engine are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [v0.6.1] — 2026-09-04 (patch 2)

### Added
- `README.md` — Full rewrite for v0.6.1: 14-section comprehensive documentation covering Phase 2 Exponential Backoff Retry (retry sequence table, telemetry tag reference, configuration, v0.6.1 fatal re-raise rationale) and Phase 3 DLQ & Replay (DLQ entry schema, inspection commands, replay internals code block, idempotency guarantee table, full outage recovery workflow); updated architecture ASCII diagram with `asyncflow:dlq` Redis key and `POST /dlq/replay` client path; updated API reference with `GET /dlq`, `POST /dlq/replay` full endpoint docs including `DlqReplayResponse` field table and safety guarantees; updated component status table with Phase 2/3 entries; version badge updated to `0.6.1`

---

## [v0.6.1] — 2026-09-04

### Fixed
- `app/worker/queue_tasks.py` — **Critical**: `process_workflow` was catching `LLMConnectionError` (and all other fatal step exceptions), recording the step as `FAILED`, then **returning normally** with a result dict. RQ interpreted a normal `return` as a successful job completion, bypassing exponential backoff retry and DLQ routing entirely. Fix: the causal exception is now saved into `fatal_exc` before each `break` in the step execution loop; after building skip records for remaining steps, the function executes `raise fatal_exc`, giving RQ the unhandled exception it requires to transition the job to `[failed]` and activate the Retry + `on_failure` DLQ machinery.

### Changed
- `app/worker/queue_tasks.py` — Module docstring updated to v0.6.1; fault model table updated to document `**exception RE-RAISED**` behaviour for all error types; `process_workflow` `Raises` section fully populated with all re-raised exception types and a `Why re-raise` design rationale; final success log changed from `"finished. Final status: FAILED/COMPLETED"` to `"finished successfully — N step(s) completed."` (the FAILED path now always raises instead of falling through to this line)
- `app/main.py` — Version bumped `0.6.0` → `0.6.1`; app description updated with critical fix blurb
- `reports/.gitignore` — Whitelisted `!v0.6.1_report.md`

---

## [v0.6.0] — 2026-09-04

### Added
- `app/worker/dlq.py` — `replay_dlq(redis_conn, rq_queue)` function: Phase 3 DLQ Replay engine; iteratively `RPOP`s entries from `asyncflow:dlq` (oldest-first) and re-enqueues each `payload` as a fresh RQ job via `queue.enqueue(process_workflow, payload, retry=Retry(...), on_failure=route_to_dlq)`; a fresh `Retry()` object resets `retries_left` to `RQ_RETRY_MAX` — idempotent re-entry with no carry-over failure metadata; malformed entries (non-JSON, missing `payload`) are skipped and permanently dropped (not restored) to prevent poison-pill infinite loops; enqueue failures restore the raw entry to the DLQ tail via `RPUSH` to prevent silent data loss; returns `{requeued, skipped, total_processed, skipped_details}` dict
- `app/main.py` — `POST /dlq/replay` endpoint (`replay_dlq_endpoint`): admin route mounted at top-level (not under `/workflow` prefix) to avoid wildcard collision; offloads synchronous `replay_dlq()` to `asyncio.run_in_executor` to keep the async event loop unblocked; returns `DlqReplayResponse` JSON with precise requeue/skip counts and per-skip diagnostic strings; returns 503 on unexpected Redis error; empty-DLQ case returns a descriptive message without error
- `app/core/schemas.py` — `DlqReplayResponse` Pydantic model: fields `requeued: int`, `skipped: int`, `total_processed: int`, `message: str`, `skipped_details: list[str]`; full field-level descriptions for OpenAPI documentation

### Changed
- `app/worker/dlq.py` — Module docstring updated to v0.6.0; added `Why RPOP for replay` and `Thread-safety` (extended) sections; new imports: `from rq import Queue`, `from rq.job import Retry`
- `app/main.py` — Version bumped `0.5.1` → `0.6.0`; app description updated with Phase 3 blurb; imports extended with `partial` from `functools`, `DlqReplayResponse` from schemas, `replay_dlq` from worker; module docstring updated with v0.6.0 additions block
- `app/api/routes.py` — Module docstring updated to v0.6.0; `POST /dlq/replay` added to endpoint surface listing
- `app/core/schemas.py` — Module docstring version marker updated `V1` → `v0.6.0`

---

## [v0.5.1] — 2026-09-03

### Added
- `app/core/config.py` — `DLQ_MAX_ENTRIES: int = 500` new config field; upper bound on entries returned by `GET /dlq` and `GET /workflow/dlq` to prevent large wire transfers during outage storms; environment-overridable via `.env`
- `app/main.py` — `GET /dlq` top-level route (`get_dlq_top_level`): mounted directly on the `app` instance (not under `/workflow` prefix); supports optional `limit` query parameter (1–1000, default `DLQ_MAX_ENTRIES`); uses `LRANGE dlq_key 0 (limit-1)` for O(limit) fetch; returns 503 on Redis error, `[]` on empty DLQ, malformed entries surfaced as `{"error": "malformed DLQ entry"}`
- `reports/v0.5.1_report.md` — Patch report: route table, pagination design rationale, `_on_job_retried` wiring root cause analysis, full telemetry lifecycle trace, test commands, verification checklist, and carried-forward known limitations

### Changed
- `app/api/routes.py` — `get_dlq_entries()` (`GET /workflow/dlq`): added `limit: int = Query(default=None, ge=1, le=1000)` parameter; `LRANGE asyncflow:dlq 0 -1` replaced with bounded `LRANGE asyncflow:dlq 0 (cap-1)` where `cap = limit or DLQ_MAX_ENTRIES`; added `Query` to FastAPI import; module docstring updated to v0.5.1 with top-level `/dlq` note
- `app/main.py` — Version bumped `0.5.0` → `0.5.1`; app description updated; imports extended with `json`, `HTTPException`, `Query`, `Request`, `status`; module docstring updated with v0.5.1 additions block
- `app/core/config.py` — Module docstring updated with `Patch (v0.5.1)` section; Unicode arrow `→` fixed to ASCII `->` in comment (prevents `cp1252` encoding errors on Windows)
- `run_worker.py` — `_on_job_retried` callback is now properly wired into `SimpleWorker` via a `_patched_handle_failure` closure monkeypatched onto `worker.handle_job_failure`; intercepts retried jobs (`job.retries_left > 0`) and emits `[RETRYING]` log before delegating to original RQ handler; wrapped in `try/AttributeError` for graceful degradation if RQ internals change; startup log confirms wiring; module docstring updated with `Patch (v0.5.1)` section
- `reports/.gitignore` — Whitelisted `!v0.5.1_report.md`

### Fixed
- `run_worker.py` — `_on_job_retried` was defined but never registered with `SimpleWorker`; no `[RETRYING]` RQ-layer telemetry was emitted on job failures; fixed via `handle_job_failure` monkeypatch
- `app/api/routes.py` — `GET /workflow/dlq` fetched the entire DLQ list unboundedly (`LRANGE 0 -1`); replaced with `LRANGE 0 (cap-1)` bounded by `DLQ_MAX_ENTRIES`

---

## [v0.5.0] — 2026-09-02


### Added
- `app/worker/dlq.py` — New Dead-Letter Queue router module; exports `route_to_dlq(job, connection, type_, value, traceback)` — a fully RQ-compatible `on_failure` callback; serialises failed job payload + traceback + timestamps into a structured JSON envelope and pushes to `asyncflow:dlq` via `redis_conn.lpush()`; handles DLQ push failure with `CRITICAL`-level log to prevent silent payload loss; uses `LPUSH` so newest failures appear at head for natural inspection order
- `app/core/config.py` — Three new settings: `RQ_RETRY_MAX: int = 3` (max RQ-level job retries), `RQ_RETRY_INTERVALS: list[int] = [2, 4, 8]` (exponential backoff delays in seconds), `DLQ_REDIS_KEY: str = "asyncflow:dlq"` (Redis list key for permanently failed payloads); all values are environment-overridable via `.env`
- `app/api/routes.py` — `GET /workflow/dlq` endpoint: fetches all permanently failed payloads from `asyncflow:dlq` via `LRANGE 0 -1`, deserialises each entry, returns newest-first list; malformed entries surfaced as `{"error": "malformed DLQ entry"}` instead of crashing; returns 503 if Redis unreachable; returns `[]` when DLQ is empty
- `reports/v0.5.0_report.md` — Full Phase 2 Retry + DLQ implementation report: architecture data flow diagram, retry mechanics deep dive (4-attempt table with exponential delay rationale), DLQ data model (Redis key structure + full entry schema), worker telemetry log examples for happy path / step retry / job DLQ routing, file-by-file change summary, and verification checklist

### Changed
- `app/api/routes.py` — `submit_workflow()`: now imports `Retry` from `rq.job` and `route_to_dlq` from `app.worker.dlq`; builds `retry_policy = Retry(max=settings.RQ_RETRY_MAX, interval=settings.RQ_RETRY_INTERVALS)` per-submission; passes `retry=retry_policy` and `on_failure=route_to_dlq` to `queue.enqueue()`; submit log upgraded to `[QUEUED]` tag including retry policy parameters; module docstring updated to document DLQ endpoint and Retry wiring
- `app/worker/queue_tasks.py` — Module docstring expanded with job-level fault model and full telemetry lifecycle table; `Worker processing workflow` log upgraded to `[PROCESSING]` tag; step retry warning split into `[RETRYING]` (when `attempt < max_attempts`) and plain warning (on final failure); workflow completion log tagged `[PROCESSING]`
- `run_worker.py` — Full rewrite: imports `Retry` from `rq.job` and `route_to_dlq`; adds `_on_job_started()` and `_on_job_retried()` telemetry callbacks; startup logs now print retry policy (`max`, `intervals`) and DLQ key for operator visibility; docstring documents all four telemetry lifecycle tags (`[QUEUED]` / `[PROCESSING]` / `[RETRYING]` / `[DLQ-SENT]`) and exponential backoff rationale for GPU bottleneck scenarios
- `app/main.py` — Version bumped `0.1.0` → `0.5.0`; app description extended to mention Retry + DLQ; module docstring updated with v0.5.0 additions
- `reports/.gitignore` — Whitelisted `v0.5.0_report.md` so the Phase 2 DLQ report is tracked by git

---

## [v0.9] — 2026-08-22

### Changed
- `README.md` — Full rewrite for project completion: replaced stub development-status table with a complete production README; added ASCII architecture diagram (Client -> FastAPI -> Redis -> SimpleWorker -> Ollama -> Redis -> Client), full directory tree, 6-step quick-start guide, complete API reference for all 3 endpoints (submit, status, health), workflow and per-step lifecycle diagrams, detailed config reference for all 6 task types (summarize, sentiment, extract_json, translate, classify, custom_prompt), payload chaining and `input_override` explanation with ASCII flow diagram, fault model table (LLMConnectionError / LLMTimeoutError / LLMMalformedResponseError), hardware constraints table (RTX 5060 VRAM budget), full environment variable reference including `PYTHONUTF8`, annotated end-to-end example run, and component status table (all 8 components marked complete)


## [v0.8] — 2026-08-22

### Fixed
- `app/worker/engine.py` — Replaced Unicode `→` (U+2192) with ASCII `->` in `_post()` debug log (`LLMEngine POST ->`) and `←` (U+2190) with `<-` in the response debug log (`LLMEngine <-`); Windows PowerShell defaults to `cp1252` which cannot encode these codepoints at the stream handler level, causing `UnicodeEncodeError` even when `PYTHONUTF8=1` is not set in the process environment
- `app/worker/queue_tasks.py` — Replaced Unicode `→` with ASCII `->` in `_run_llm_step()` debug log (`Step '%s' ->`); same cp1252 root cause as above; all three affected logger call sites are now pure ASCII
- `payload.json` — Confirmed `input_override` present on `step_extract` with the exact original financial text; ensures the EXTRACT_JSON step receives the seed `input_text` directly rather than the chained sentiment output (`"positive | 0.87"`) from `step_sentiment`

## [v0.7] — 2026-08-21

### Added
- `reports/v0.4.1_report.md` — Phase 2 live test report (job `f4d5cb76`): exact command outputs for all 5 prerequisites, worker log trace for all 3 steps, full API response JSON, performance table (30.49s total / 43+7+41 tokens), root cause analysis for 2 bugs found during testing

### Fixed
- `.env` — Added `PYTHONUTF8=1`; resolves `UnicodeEncodeError: 'charmap' codec can't encode '\u2192'` on Windows `cp1252` console when DEBUG-level log lines containing arrow glyphs are emitted from `engine.py` and `queue_tasks.py`; non-fatal at runtime but pollutes stderr and obscures actual log output
- `payload.json` — Added `input_override` to `step_extract`; step was receiving chained sentiment output (`"positive | 0.87"`) instead of original financial text, causing correct but semantically mismatched extraction; `input_override` bypasses chain for this step and feeds the original `input_text` directly

## [v0.6] — 2026-08-21

### Added
- `app/worker/prompts.py` — Phase 2 prompt engineering module; exports `build_prompt(step, input_text) → PromptPackage`; implements per-task system instruction builders for all six `TaskType` variants; uses `dict[TaskType, Callable]` dispatch table (O(1)) instead of `if/elif` chain; `PromptPackage` is a frozen dataclass (`frozen=True, slots=True`) with `system`, `user`, `use_json_mode`, `temperature`, `max_tokens` fields
- `reports/v0.4_report.md` — Phase 2 build report: architecture diagram, temperature policy rationale, VRAM budget math (RTX 5060 8 GB KV-cache calculation), `format="json"` mechanism deep-dive, end-to-end test guide with PowerShell validation commands, known limitations table

### Changed
- `app/worker/engine.py` — Full production implementation of `LLMEngine` replacing Phase 1 stub: added `generate(prompt, system, options) → str` and `generate_json(prompt, system, schema, options) → dict`; internal `_build_payload()` assembles Ollama `POST /api/generate` body with `stream=False`, `num_predict` hard-capped at `OLLAMA_MAX_TOKENS`, `format="json"` only set for JSON-mode calls; `_post()` uses synchronous `httpx.Client` with one automatic retry on HTTP 5xx (covers Ollama cold-start); custom exception hierarchy: `LLMConnectionError`, `LLMTimeoutError`, `LLMMalformedResponseError`
- `app/worker/queue_tasks.py` — Phase 2 rewrite: removed `_simulate_step()`, `_SIMULATED_LATENCY_SECONDS`, and `time.sleep()`; added `_run_llm_step(engine, step, input_text) → str` dispatcher that calls `build_prompt()` then `LLMEngine.generate()` or `.generate_json()`; `LLMConnectionError` is treated as fatal (bypasses retry loop, aborts entire workflow immediately); `LLMTimeoutError` and `LLMMalformedResponseError` are retryable via existing `retry_on_failure` mechanism; one `LLMEngine` instance reused across all steps per workflow; `EXTRACT_JSON` output serialised via `json.dumps()` to maintain string chaining contract
- `app/core/config.py` — Added `OLLAMA_MAX_TOKENS: int = 4096` (hard `num_predict` cap for 8 GB VRAM KV-cache budget) and `OLLAMA_TEMPERATURE_DEFAULT: float = 0.3` (fallback; overridden per-task by `prompts.py`)

## [v0.5] — 2026-08-21

### Added
- `payload.json` — Shell-escape-proof test payload for Windows `curl.exe -d @payload.json` usage
- `run_worker.py` — Windows-compatible RQ worker launcher using `SimpleWorker` (avoids `os.fork()` POSIX limitation); includes Redis ping on startup, structured logging, and documented trade-offs vs default forking Worker
- `reports/v0.3.1_report.md` — Full end-to-end test report with three bug root cause analyses and live response JSON

### Fixed
- `app/api/routes.py` — `workflow_name` extraction in `get_workflow_status()`: RQ stores positional args in `job.args[0]` (a tuple), not `job.kwargs["request_dict"]`; fixed to read from `job.args[0]` with kwargs fallback
- `run_worker.py` — Fixed `SyntaxWarning` for invalid `\S` escape sequence by using raw string (`r"""..."""`) docstring

## [v0.4] — 2026-08-21

### Fixed
- `app/core/schemas.py` — `WorkflowSubmitRequest.metadata` field: removed double-default conflict (`default_factory=dict` inside `Field()` AND `= {}` class-level default) that caused `TypeError: cannot specify both default and default_factory` on Pydantic 2.13.4; unified to single `Field(default_factory=dict)` declaration
- `app/api/routes.py` — `_RQ_STATUS_MAP`: replaced `JobStatus.STOPPED` (RQ 1.x) with `JobStatus.CANCELED` (RQ 2.x single-L spelling); added `JobStatus.CREATED` (new pre-queue state in RQ 2.x)

### Added
- `reports/v0.3_report.md` — Root cause analysis for both startup crash bugs, before/after diffs, live server verification log, and `pip freeze` recommendation

## [v0.3] — 2026-08-21

### Added
- `venv/` — Isolated Python 3 virtual environment (excluded from git via `.gitignore`)
- `reports/v0.2_report.md` — Environment lock-in report with full dependency resolution table and install log

### Changed
- `requirements.txt` — Switched from `==` exact pins to `>=` minimum-version pins; added inline rationale comments; pinned floors: `fastapi>=0.103.0`, `pydantic>=2.4.0`, `rq>=1.15.0`, `redis>=5.0.0`; resolved to 34 packages total
- `.env` — Updated `LOG_LEVEL` to `DEBUG` for local development verbosity; `REDIS_URL` corrected to `redis://localhost:6379` (no DB suffix); all Ollama Phase 2 vars retained

## [v0.2] — 2026-08-21

### Added
- `reports/v0.1_report.md` — Phase 1 build report (Redis/RQ wiring, state machine, test guide)

### Changed
- `app/main.py` — Implemented Redis connection pool in `lifespan()`: sync `redis.Redis` client with 5s connect timeout, `ping()` liveness check, `app.state` injection of `redis_conn` and `rq_queue`; fail-fast `ConnectionError` propagation prevents the app from starting with a dead queue
- `app/api/routes.py` — Full RQ wiring: `POST /workflow/submit` now enqueues via `app.state.queue.enqueue()` and returns the RQ job ID as `task_id`; `GET /workflow/{task_id}/status` uses `Job.fetch()` with `NoSuchJobError → 404` and Redis error → 503; full RQ→WorkflowStatus mapping table; `_build_status_response()` reconstructs `StepResult` objects from job result dict
- `app/worker/queue_tasks.py` — Implemented `process_workflow()` simulation loop: 2s sleep per step, chained I/O (output → next step input), `input_override` support, per-step retry logic (`retry_on_failure`), skip propagation on failure, task-type-specific mock outputs, `model_dump(mode="json")` for datetime-safe serialisation

## [v0.1] — 2026-08-21

### Added
- Full project directory structure: `app/`, `app/api/`, `app/core/`, `app/worker/`, `reports/`
- `app/main.py` — FastAPI application with async lifespan, CORS middleware, `/health` probe
- `app/api/routes.py` — `POST /workflow/submit` (202) and `GET /workflow/{task_id}/status` stubs
- `app/core/config.py` — `pydantic-settings` environment config with `lru_cache` singleton
- `app/core/schemas.py` — Full Pydantic V2 schema layer:
  - `TaskType`, `WorkflowStatus`, `StepStatus` enumerations
  - Six discriminated `StepConfig` variants: `SummarizeConfig`, `TranslateConfig`, `ExtractJsonConfig`, `ClassifyConfig`, `SentimentConfig`, `CustomPromptConfig`
  - `WorkflowStep`, `WorkflowSubmitRequest`, `WorkflowSubmitResponse`, `StepResult`, `WorkflowStatusResponse`
  - Field validators: `step_ids_must_be_unique`, `validate_translate_not_first`
- `app/worker/engine.py` — `LLMEngine` stub (Ollama interface, Phase 2)
- `app/worker/queue_tasks.py` — `process_workflow` RQ task stub (Phase 2)
- `docker-compose.yml` — Redis 7 Alpine with AOF persistence, health check, optional RedisInsight
- `requirements.txt` — Pinned dependencies: FastAPI, Uvicorn, Pydantic, RQ, Redis, httpx, pytest
- `.env` — Environment variable template (no secrets)
- `.gitignore` — Python, venv, IDE, Docker, logs coverage
- `README.md` — Architecture diagram, quick-start guide, task type table, example payload
- `reports/.gitignore` — Self-isolating report folder
- `reports/v0.0_report.md` — Full V0.0 build report with design decisions and rationale
