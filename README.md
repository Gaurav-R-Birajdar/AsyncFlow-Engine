# AsyncFlow Engine

> A production-grade, multi-step LLM workflow orchestration engine built with **FastAPI**, **Redis Queue (RQ)**, and a local **Ollama / Llama 3.1** inference server — with enterprise-grade resilience via exponential-backoff retry, a Redis Dead-Letter Queue (DLQ), and a one-call DLQ Replay mechanism.

Submit a JSON payload describing a chain of NLP tasks. The engine validates it, queues it in Redis, executes each step sequentially through the local LLM, retries transient failures automatically, routes permanently failed jobs to a Dead-Letter Queue for inspection, and lets an admin replay them back into the live queue — all without blocking the original caller.

[![Version](https://img.shields.io/badge/version-0.6.1-blue)](CHANGELOG.md)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.100%2B-green)](https://fastapi.tiangolo.com/)
[![Redis](https://img.shields.io/badge/Redis-7-red)](https://redis.io/)

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Directory Structure](#2-directory-structure)
3. [Quick Start](#3-quick-start)
4. [API Reference](#4-api-reference)
5. [Workflow Lifecycle](#5-workflow-lifecycle)
6. [Step Types & Configuration](#6-step-types--configuration)
7. [Payload Chaining & input_override](#7-payload-chaining--input_override)
8. [Phase 2 — Exponential Backoff Retry](#8-phase-2--exponential-backoff-retry)
9. [Phase 3 — Dead-Letter Queue & Replay](#9-phase-3--dead-letter-queue--replay)
10. [Fault Model (Complete)](#10-fault-model-complete)
11. [Hardware Constraints](#11-hardware-constraints)
12. [Environment Variables](#12-environment-variables)
13. [Example End-to-End Run](#13-example-end-to-end-run)
14. [Component Status](#14-component-status)

---

## 1. Architecture

```
+------------------------------------------------------------------------------+
|                     CLIENT  (curl / PowerShell / app)                        |
+--------------------------------------+---------------------------------------+
                                       |  POST /workflow/submit
                                       v
+------------------------------------------------------------------------------+
|              FastAPI  (app/main.py + app/api/routes.py)                      |
|                                                                              |
|  1. Pydantic V2 validates payload (discriminated union, unique step IDs)     |
|  2. Serialises model -> plain dict  (pickle-safe for RQ)                     |
|  3. queue.enqueue(process_workflow, payload_dict,                            |
|       retry=Retry(max=3, interval=[2,4,8]),    <- Phase 2: backoff           |
|       on_failure=route_to_dlq)                 <- Phase 3: DLQ routing       |
|  4. Returns HTTP 202 ACCEPTED + task_id immediately                          |
+--------------------------------------+---------------------------------------+
                                       |  enqueue
                                       v
+------------------------------------------------------------------------------+
|                     Redis  (redis:7-alpine / AOF persistence)                |
|                                                                              |
|  Queue:  asyncflow_default   (active jobs — BLMOVE dequeue)                  |
|  Key:    asyncflow:dlq       (permanently failed payloads — Redis List)      |
|  Stores: job payload, status, result, exception info, retry counters         |
+--------------------------------------+---------------------------------------+
                                       |  dequeue
                                       v
+------------------------------------------------------------------------------+
|             RQ SimpleWorker  (run_worker.py)                                 |
|             [Windows: no os.fork() -- runs in-process]                       |
|                                                                              |
|  process_workflow():                                                         |
|    for each step:                                                            |
|      input = input_override ?? previous_output ?? seed_text                 |
|      prompts.build_prompt(step, input) -> PromptPackage                     |
|      LLMEngine.generate() or .generate_json() -> Ollama                     |
|                                                                              |
|      on LLMConnectionError: log, record FAILED, raise -> RQ retry           |
|      on Timeout/Malformed: retry step if retry_on_failure=True, else raise   |
|                                                                              |
|  on job exception -> retries_left > 0  -> re-enqueue (2s, 4s, 8s backoff)  |
|                   -> retries_left == 0 -> route_to_dlq() LPUSH asyncflow:dlq|
+--------------------------------------+---------------------------------------+
                                       |  HTTP POST /api/generate
                                       v
+------------------------------------------------------------------------------+
|              Ollama  (localhost:11434)  --  Llama 3.1 8B Q4_K_M             |
|              RTX 5060  |  8 GB VRAM  |  stream=False                        |
+--------------------------------------+---------------------------------------+
                                       |  result stored in Redis
                                       v
+------------------------------------------------------------------------------+
|  CLIENT -> GET /workflow/{task_id}/status   (poll results)                   |
|  ADMIN  -> GET /dlq                         (inspect failed jobs)            |
|         -> POST /dlq/replay                 (requeue all DLQ jobs)           |
+------------------------------------------------------------------------------+
```

---

## 2. Directory Structure

```
AsyncFlow Engine/
+-- app/
|   +-- main.py               # FastAPI app, lifespan, CORS, GET /dlq, POST /dlq/replay
|   +-- api/
|   |   +-- routes.py         # POST /workflow/submit, GET /workflow/{id}/status, GET /workflow/dlq
|   +-- core/
|   |   +-- config.py         # pydantic-settings, reads .env, lru_cache singleton
|   |   +-- schemas.py        # Pydantic V2 models, single source of truth for all JSON shapes
|   +-- worker/
|       +-- engine.py         # LLMEngine, Ollama HTTP client, retry on 5xx
|       +-- prompts.py        # build_prompt(), per-task system instruction builders
|       +-- queue_tasks.py    # process_workflow(), RQ entry point, chaining, fatal re-raise
|       +-- dlq.py            # route_to_dlq() on_failure callback; replay_dlq() Phase 3
+-- run_worker.py             # Windows-compatible SimpleWorker launcher, retry/DLQ wiring
+-- docker-compose.yml        # Redis 7-Alpine + optional RedisInsight
+-- payload.json              # Ready-to-run 3-step demo payload
+-- requirements.txt          # Pinned >= floors for all dependencies
+-- .env                      # Local environment overrides (not committed)
+-- reports/                  # Per-version implementation reports
+-- CHANGELOG.md              # Keep-a-Changelog format version history
```

---

## 3. Quick Start

### Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| Docker Desktop | Any recent |
| Ollama | Latest |
| Llama 3.1 model | `ollama pull llama3.1` |

### Step 1 — Clone and create virtualenv

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Step 2 — Start Redis

```powershell
docker-compose up -d
# Optional: RedisInsight UI on port 8001
docker-compose --profile debug up -d
```

### Step 3 — Start Ollama

```powershell
ollama serve          # separate terminal
ollama pull llama3.1  # first time only
```

### Step 4 — Start the API server

```powershell
$env:PYTHONUTF8 = "1"
uvicorn app.main:app --reload
# API:     http://localhost:8000
# Swagger: http://localhost:8000/docs
```

### Step 5 — Start the RQ worker

```powershell
.\venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
python run_worker.py
```

> **Why `run_worker.py`?** RQ's default worker calls `os.fork()` — POSIX-only, crashes on Windows.
> `run_worker.py` uses `rq.SimpleWorker`, the officially documented Windows workaround.

### Step 6 — Submit a workflow

```powershell
curl.exe -X POST http://localhost:8000/workflow/submit `
  -H "Content-Type: application/json" -d "@payload.json"
```


---

## 4. API Reference

| Method | Path | Purpose | Tag |
|---|---|---|---|
| `POST` | `/workflow/submit` | Validate & enqueue a workflow | Workflow |
| `GET` | `/workflow/{task_id}/status` | Poll execution state & results | Workflow |
| `GET` | `/workflow/dlq` | Inspect DLQ entries (legacy alias) | Dead-Letter Queue |
| `GET` | `/dlq` | Inspect DLQ entries (top-level, paginated) | Dead-Letter Queue |
| `POST` | `/dlq/replay` | Drain DLQ, requeue all as fresh jobs | Dead-Letter Queue |
| `GET` | `/health` | Liveness probe | Meta |

### POST /workflow/submit

**Request body:**
```json
{
  "workflow_name": "my-pipeline",
  "input_text": "The source text to process...",
  "steps": [
    { "step_id": "step_1", "config": { "task_type": "summarize", "max_words": 100 } },
    { "step_id": "step_2", "config": { "task_type": "sentiment" } }
  ],
  "metadata": {}
}
```

**Response `202 Accepted`:**
```json
{
  "task_id": "d3f1a2b4-...",
  "status": "queued",
  "message": "Workflow 'my-pipeline' accepted — 2 step(s) queued. Poll /workflow/d3f1a2b4-.../status for updates."
}
```

| Error | Cause |
|---|---|
| `422` | Pydantic validation failed — bad schema, duplicate `step_id`, `translate` as first step |
| `503` | Redis unreachable |

### GET /workflow/{task_id}/status

**Response `200 OK`:**
```json
{
  "task_id": "d3f1a2b4-...",
  "workflow_name": "phase2-full-pipeline-test",
  "status": "completed",
  "step_results": [
    {
      "step_id": "step_summarize",
      "status": "completed",
      "output": "Q3 revenue $4.2B, +18% YoY. Margins 24%. $500M buyback approved.",
      "error": null,
      "started_at": "2026-09-04T13:07:01Z",
      "finished_at": "2026-09-04T13:07:13Z"
    }
  ],
  "final_output": "{\"revenue_billion_usd\": 4.2, ...}",
  "submitted_at": "2026-09-04T13:07:01Z",
  "completed_at": "2026-09-04T13:07:30Z",
  "error": null
}
```

| Error | Cause |
|---|---|
| `404` | `task_id` not found or expired |
| `503` | Redis unreachable |

### GET /dlq

Inspect all permanently failed payloads from `asyncflow:dlq`.

**Query Parameters:**

| Param | Type | Default | Description |
|---|---|---|---|
| `limit` | int (1-1000) | `500` | Max entries to return (newest-first) |

**Response `200 OK`** — list of DLQ entry objects:
```json
[
  {
    "job_id": "63093cdd-...",
    "workflow_name": "phase2-full-pipeline-test",
    "enqueued_at": "2026-09-04T13:07:01Z",
    "failed_at": "2026-09-04T13:07:06Z",
    "attempt": 4,
    "traceback": "Traceback...\nLLMConnectionError: Cannot connect to Ollama...",
    "payload": { "workflow_name": "...", "input_text": "...", "steps": [...] }
  }
]
```

Returns `[]` when the DLQ is empty.

### POST /dlq/replay

Drain the DLQ and requeue every entry as a fresh job with a reset retry counter.

**Response `200 OK`:**
```json
{
  "requeued": 3,
  "skipped": 0,
  "total_processed": 3,
  "message": "Replay complete: 3 job(s) successfully re-enqueued.",
  "skipped_details": []
}
```

| Field | Type | Description |
|---|---|---|
| `requeued` | int | Jobs pushed back into `asyncflow_default` |
| `skipped` | int | Entries that could not be re-enqueued |
| `total_processed` | int | `requeued + skipped` |
| `message` | string | Human-readable summary |
| `skipped_details` | list[str] | Per-skip diagnostic reason strings |

### GET /health

```json
{ "status": "ok", "version": "0.6.1" }
```

---

## 5. Workflow Lifecycle

### Job-Level States

```
 POST /workflow/submit
         |
         v
    [ QUEUED ] ──── worker picks up ───> [ RUNNING ]
                                               |
                     +─────────────────────────+─────────────────────+
                     v                         v                     v
               [ COMPLETED ]            [ FAILED ]            [ CANCELLED ]
            all steps finished      exception raised
                                           |
                              retries_left > 0  ->  re-enqueue (Phase 2 backoff)
                              retries_left == 0 ->  route_to_dlq()  (Phase 3 DLQ)
                                                          |
                                                  POST /dlq/replay
                                                          |
                                                    [ QUEUED ] (fresh job)
```

### Per-Step States

```
PENDING -> RUNNING -> COMPLETED
                   \-> FAILED  -> all subsequent steps become SKIPPED
```

---

## 6. Step Types & Configuration

All steps share these top-level fields:

| Field | Type | Default | Description |
|---|---|---|---|
| `step_id` | string | required | Unique slug `[a-zA-Z0-9_-]`, max 64 chars |
| `config` | object | required | Task-specific configuration |
| `input_override` | string or null | `null` | Inject explicit input, bypassing chaining |
| `retry_on_failure` | boolean | `false` | Retry this step once on timeout or malformed JSON |

### `summarize`

```json
{ "task_type": "summarize", "max_words": 60, "style": "bullet points" }
```

| Field | Range | Default |
|---|---|---|
| `max_words` | 10–2000 | `150` |
| `style` | any string | `"narrative"` |

Style hints: `"bullet points"`, `"narrative"`, `"executive"`, `"tldr"`

### `sentiment`

```json
{ "task_type": "sentiment", "granularity": "document" }
```

Output format: `"positive | 0.92"`

### `extract_json`

```json
{
  "task_type": "extract_json",
  "strict": true,
  "output_schema": {
    "type": "object",
    "properties": {
      "revenue_billion_usd": { "type": "number" },
      "yoy_growth_pct":      { "type": "number" }
    },
    "required": ["revenue_billion_usd", "yoy_growth_pct"]
  }
}
```

Uses Ollama `format="json"` to constrain token sampling to valid JSON.

### `translate`

```json
{ "task_type": "translate", "target_language": "fr", "preserve_formatting": true }
```

> **Constraint:** Cannot be the first step. Prepend `summarize` or `custom_prompt`.

### `classify`

```json
{ "task_type": "classify", "labels": ["finance", "technology"], "multi_label": false }
```

### `custom_prompt`

```json
{
  "task_type": "custom_prompt",
  "prompt_template": "Rewrite as a formal board memo:\n\n{{ input }}",
  "temperature": 0.5,
  "max_tokens": 1024
}
```

---

## 7. Payload Chaining & `input_override`

By default, each step receives the **output of the previous step** as its input:

```
input_text -> step_1 --output--> step_2 --output--> step_3
```

Use `input_override` to break the chain and inject arbitrary text for a specific step:

```
input_text ──────────────────────────────────────────> step_3 (via input_override)
    |
    v
 step_1 --output--> step_2
```

**Example:** In `payload.json`, `step_extract` uses `input_override` to receive the original
financial paragraph instead of the `"positive | 0.87"` string from `step_sentiment`.

---

## 8. Phase 2 — Exponential Backoff Retry

### How It Works

Every `POST /workflow/submit` call attaches a `Retry` policy at enqueue time:

```python
queue.enqueue(
    process_workflow,
    payload_dict,
    retry=Retry(max=3, interval=[2, 4, 8]),  # 3 retries with growing delays
    on_failure=route_to_dlq,                  # fires after the 4th failure
)
```

### Retry Sequence (Ollama Offline Example)

```
Attempt 1  FAILED -> retries_left=3 -> re-enqueue, wait  2 seconds
Attempt 2  FAILED -> retries_left=2 -> re-enqueue, wait  4 seconds
Attempt 3  FAILED -> retries_left=1 -> re-enqueue, wait  8 seconds
Attempt 4  FAILED -> retries_left=0 -> route_to_dlq() fires
```

Total backoff window: **14 seconds** (2+4+8) before DLQ routing.

### Why Exponential Backoff?

| Scenario | Why Backoff Helps |
|---|---|
| GPU thermal throttle | Gives GPU time to cool between attempts |
| Ollama cold-start | 2-4s is enough for model weights to become responsive |
| Transient network blip | Avoids hammering a recovering service immediately |
| Permanent outage | 14s window, then DLQ captures payload for replay |

### Worker Telemetry Tags

| Tag | Where | Meaning |
|---|---|---|
| `[QUEUED]` | `routes.py` | Job accepted into Redis queue |
| `[PROCESSING]` | `queue_tasks.py` | Worker started executing |
| `[RETRYING]` | `run_worker.py` | RQ re-queuing with backoff delay |
| `[DLQ-SENT]` | `dlq.py` | Final failure; payload pushed to `asyncflow:dlq` |
| `[REPLAY]` | `dlq.py`, `main.py` | Admin replay triggered; jobs re-enqueued |

### Configuration

Override in `.env`:
```ini
RQ_RETRY_MAX=5
RQ_RETRY_INTERVALS=[1, 2, 4, 8, 16]
```

### Critical Detail: Why Raising Matters (v0.6.1 Fix)

RQ's retry machinery **only activates when the worker function raises an unhandled exception**.
A normal Python `return` — even with `status: FAILED` — is treated as job success.

`process_workflow` was previously swallowing `LLMConnectionError` and returning normally.
v0.6.1 fixes this by re-raising the causal exception after recording step results:

```python
# queue_tasks.py — after logging and recording skip entries:
raise fatal_exc   # <- RQ sees this; activates retry + DLQ routing
```

---

## 9. Phase 3 — Dead-Letter Queue & Replay

### What Is the DLQ?

The Dead-Letter Queue is a **Redis List** (`asyncflow:dlq`) that permanently stores
the full payload and traceback of every job that exhausted all retry attempts.
No failed workflow is ever silently discarded.

### DLQ Entry Schema

| Field | Type | Description |
|---|---|---|
| `job_id` | string | Original RQ job UUID |
| `workflow_name` | string or null | From the submitted payload |
| `enqueued_at` | ISO-8601 | When the job was first submitted |
| `failed_at` | ISO-8601 | When the final failure was detected |
| `attempt` | int | Total attempts made (default: 4) |
| `traceback` | string | Full exception traceback from last failure |
| `payload` | dict | Complete original `WorkflowSubmitRequest` dict |

### Inspecting the DLQ

```powershell
# Fetch all entries (newest-first, max 500)
curl.exe -s http://localhost:8000/dlq | python -m json.tool

# Paginate
curl.exe -s "http://localhost:8000/dlq?limit=10"

# Direct Redis inspection
docker exec -it asyncflow_redis redis-cli LLEN asyncflow:dlq
docker exec -it asyncflow_redis redis-cli LRANGE asyncflow:dlq 0 -1
```

### The Replay Mechanism

```powershell
curl.exe -X POST http://localhost:8000/dlq/replay

# Response:
{
  "requeued": 3,
  "skipped": 0,
  "total_processed": 3,
  "message": "Replay complete: 3 job(s) successfully re-enqueued.",
  "skipped_details": []
}
```

### How Replay Works Internally

```python
# app/worker/dlq.py — replay_dlq()
while True:
    raw = redis_conn.rpop(dlq_key)   # Atomic, oldest-first (RPOP from tail)
    if raw is None:
        break                         # DLQ fully drained

    entry = json.loads(raw)
    rq_queue.enqueue(
        process_workflow,
        entry["payload"],
        job_id=None,                              # Fresh UUID — brand new identity
        retry=Retry(max=3, interval=[2, 4, 8]),   # Resets retries_left counter
        on_failure=route_to_dlq,                  # Re-arms DLQ for subsequent failures
    )
```

### Idempotency & Safety Guarantees

| Guarantee | Mechanism |
|---|---|
| No double-requeue | `RPOP` is atomic — concurrent replays pop disjoint entries |
| Retry counter fully reset | Fresh `Retry()` object — `retries_left` starts at `RQ_RETRY_MAX` |
| Fresh job identity | New UUID — avoids collision with original failed job in Redis |
| No silent data loss | If `enqueue()` throws, raw bytes `RPUSH`-ed back to DLQ tail |
| Poison-pill prevention | Malformed entries permanently dropped after WARNING log |
| DLQ re-armed | `on_failure=route_to_dlq` re-attached — re-fails go back to DLQ |

### Full Outage Recovery Workflow

```
1. Ollama goes offline
2. Jobs hit 4 attempts (2s + 4s + 8s backoff) -> DLQ
   GET /dlq -> [{ payload, traceback, ... }]

3. Restore Ollama: ollama serve
4. POST /dlq/replay
   -> each DLQ entry becomes a fresh job in asyncflow_default
   -> new retry budget: 3 retries (2s/4s/8s)

5. Worker processes replayed jobs -> success
   GET /dlq -> []
```

---

## 10. Fault Model (Complete)

### Step-Level

| Exception | Step Retry | Job-Level Behaviour |
|---|---|---|
| `LLMConnectionError` | No | Step FAILED, remaining SKIPPED, exception **re-raised** -> RQ retry/DLQ |
| `LLMTimeoutError` | Yes (if `retry_on_failure=True`) | Step retried once; on exhaustion, re-raised -> RQ retry/DLQ |
| `LLMMalformedResponseError` | Yes (if `retry_on_failure=True`) | Same as timeout |
| Any other `Exception` | Yes (if `retry_on_failure=True`) | Same retry behaviour |

### Job-Level (RQ)

| Event | Trigger | Response |
|---|---|---|
| Job exception | `process_workflow` raises | RQ checks `retries_left` |
| `retries_left > 0` | — | Re-enqueue with exponential backoff |
| `retries_left == 0` | — | `route_to_dlq()` pushes to `asyncflow:dlq` |
| DLQ push failure | Redis write error | `CRITICAL`-level log; exception detail preserved |

---

## 11. Hardware Constraints

| Constraint | Value | Reason |
|---|---|---|
| GPU | RTX 5060 | 8 GB VRAM |
| Model | Llama 3.1 8B Q4_K_M | ~4.7 GB VRAM footprint |
| KV-cache budget | ~3.3 GB | Remaining VRAM after model load |
| `num_predict` hard cap | `4096` tokens | Guards against KV-cache overflow |
| `stream` mode | `False` (always) | Blocking inference, enforces sequential execution |
| Worker concurrency | 1 (SimpleWorker) | One GPU context window at a time |

To run on Linux with a larger GPU: switch to `rq.Worker` and increase `OLLAMA_MAX_TOKENS`.

---

## 12. Environment Variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `RQ_QUEUE_NAME` | `asyncflow_default` | RQ queue name |
| `JOB_TIMEOUT` | `600` | Max worker runtime per job (seconds) |
| `RQ_RETRY_MAX` | `3` | RQ-level job retries before DLQ routing |
| `RQ_RETRY_INTERVALS` | `[2, 4, 8]` | Per-retry backoff delays (seconds) |
| `DLQ_REDIS_KEY` | `asyncflow:dlq` | Redis list key for permanently failed payloads |
| `DLQ_MAX_ENTRIES` | `500` | Max entries returned by `GET /dlq` |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1` | Model name passed to `/api/generate` |
| `OLLAMA_REQUEST_TIMEOUT` | `120` | Per-request HTTP timeout (seconds) |
| `OLLAMA_MAX_TOKENS` | `4096` | Hard `num_predict` cap (VRAM guard) |
| `OLLAMA_TEMPERATURE_DEFAULT` | `0.3` | Fallback temperature (overridden per-task by `prompts.py`) |
| `APP_ENV` | `development` | Runtime environment tag |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `PYTHONUTF8` | (set manually) | Set to `1` on Windows to prevent cp1252 encoding errors |

---

## 13. Example End-to-End Run

The included `payload.json` runs a 3-step financial analysis pipeline:

```
input_text: "The company reported Q3 revenue of $4.2 billion, up 18% YoY..."
     |
     v
step_summarize -> Bullet-point summary (~60 words)
     |
     v
step_sentiment -> "positive | 0.92"
     |
     X  (output discarded -- input_override kicks in)

input_text (original) ─────────────────────────────────────────────>
step_extract   -> { "revenue_billion_usd": 4.2, "yoy_growth_pct": 18,
                    "operating_margin_pct": 24, "buyback_million_usd": 500 }
```

### Normal Run

```powershell
# 1. Submit
curl.exe -X POST http://localhost:8000/workflow/submit `
  -H "Content-Type: application/json" -d "@payload.json"
# Response: {"task_id": "abc-123-...", "status": "queued"}

# 2. Poll
curl.exe http://localhost:8000/workflow/abc-123-.../status
# {"status": "completed", "final_output": "{\"revenue_billion_usd\": 4.2, ...}"}
```

### Outage Simulation (Phase 2 + Phase 3)

```powershell
# Stop Ollama
Stop-Process -Name "ollama" -ErrorAction SilentlyContinue

# Submit job (will fail all 4 attempts -> DLQ)
curl.exe -X POST http://localhost:8000/workflow/submit `
  -H "Content-Type: application/json" -d "@payload.json"

# Wait ~14s for backoff (2+4+8 seconds)

# Inspect DLQ
curl.exe -s http://localhost:8000/dlq | python -m json.tool
# Shows 1 entry with traceback and original payload

# Restore Ollama
Start-Process "ollama" -ArgumentList "serve"

# Replay all DLQ jobs as fresh tasks
curl.exe -X POST http://localhost:8000/dlq/replay
# {"requeued":1,"skipped":0,"total_processed":1,...}

# Confirm DLQ is now empty
curl.exe http://localhost:8000/dlq
# []
```

---

## 14. Component Status

| Component | Status | Notes |
|---|---|---|
| Pydantic V2 Schemas | Complete | Discriminated union, field validators, unique step IDs, `DlqReplayResponse` |
| FastAPI Routes | Complete | 202 submit, status polling, full RQ -> WorkflowStatus mapping |
| Redis / RQ Integration | Complete | AOF persistence, SimpleWorker, fail-fast ping on startup |
| Prompt Engineering | Complete | Per-task system instruction builders, frozen `PromptPackage` dataclass |
| LLM Engine (Ollama) | Complete | `generate()`, `generate_json()`, 1 auto-retry on HTTP 5xx |
| Phase 2 — Exponential Backoff Retry | Complete | `Retry(max=3, interval=[2,4,8])`, `[RETRYING]` telemetry, fatal re-raise fix (v0.6.1) |
| Phase 3 — Dead-Letter Queue | Complete | `route_to_dlq` on_failure callback, `GET /dlq` inspection with pagination |
| Phase 3 — DLQ Replay | Complete | `POST /dlq/replay`, atomic RPOP drain, fresh UUID + retry reset, enqueue-failure recovery |
| Windows Compatibility | Complete | `SimpleWorker`, `PYTHONUTF8=1`, ASCII-only log strings |
| Docker Infrastructure | Complete | Redis 7-Alpine, AOF, RedisInsight on `--profile debug` |

---

*Built with [Google Antigravity IDE](https://antigravity.google.dev) · AsyncFlow Engine v0.6.1*
