# Changelog

All notable changes to AsyncFlow Engine are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

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
