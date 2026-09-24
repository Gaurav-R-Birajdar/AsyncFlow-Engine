# AsyncFlow Engine

> A production-grade, multi-step LLM workflow orchestration engine built with **FastAPI**, **Redis Queue (RQ)**, and a local **Ollama / Llama 3.1** inference server — with enterprise-grade resilience via exponential-backoff retry, a Redis Dead-Letter Queue (DLQ), a one-call DLQ Replay mechanism, and an MCP Governance Interceptor that redacts PII from LLM outputs before they propagate downstream.

Submit a JSON payload describing a chain of NLP tasks. The engine validates it, queues it in Redis, executes each step sequentially through the local LLM, intercepts structured JSON outputs to enforce PII policy, retries transient failures automatically, routes permanently failed jobs to a Dead-Letter Queue for inspection, and lets an admin replay them back into the live queue — all without blocking the original caller.

[![Version](https://img.shields.io/badge/version-1.0.0-blue)](CHANGELOG.md)
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
10. [Phase 2 — MCP Governance Interceptor](#10-phase-2--mcp-governance-interceptor)
11. [Fault Model (Complete)](#11-fault-model-complete)
12. [Hardware Constraints](#12-hardware-constraints)
13. [Environment Variables](#13-environment-variables)
14. [Example End-to-End Run](#14-example-end-to-end-run)
15. [Component Status](#15-component-status)

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
|      [EXTRACT_JSON / CUSTOM_PROMPT only -- if GOVERNANCE_ENABLED=true]       |
|      GovernanceInterceptor.parse_and_sanitize(output)  <- Phase 2 SAM       |
|        detects PII keys -> redacts with [REDACTED_BY_POLICY]                |
|        emits [GOVERNANCE_INTERCEPT] telemetry if redaction occurred          |
|      AuditLogger.log_event() -> data/audit.jsonl  (append-only JSONL)       |
|                                                                              |
|      on LLMConnectionError: log, record FAILED, raise -> RQ retry           |
|      on Timeout/Malformed/GovernanceError: retry if retry_on_failure=True    |
|                                                                              |
|  on job exception -> retries_left > 0  ->  re-enqueue (2s, 4s, 8s backoff)  |
|                   -> retries_left == 0 ->  route_to_dlq() LPUSH asyncflow:dlq|
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
|   +-- governance/           # Phase 2: MCP Governance Interceptor (SAM layer)
|   |   +-- interceptor.py    # GovernanceInterceptor, EnterpriseToolPayload, GovernanceError
|   |   +-- audit.py          # AuditLogger — append-only JSONL writer to data/audit.jsonl
|   +-- worker/
|       +-- engine.py         # LLMEngine, Ollama HTTP client, retry on 5xx
|       +-- prompts.py        # build_prompt(), per-task system instruction builders
|       +-- queue_tasks.py    # process_workflow(), RQ entry point, governance injection
|       +-- dlq.py            # route_to_dlq() on_failure callback; replay_dlq() Phase 3
+-- data/
|   +-- audit.jsonl           # Append-only PII audit trail (gitignored — runtime only)
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
{ "status": "ok", "version": "1.0.0" }
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

| Tag | Level | Where | Meaning |
|---|---|---|---|
| `[QUEUED]` | INFO | `routes.py` | Job accepted into Redis queue |
| `[PROCESSING]` | INFO | `queue_tasks.py` | Worker started executing |
| `[RETRYING]` | WARNING | `run_worker.py` | RQ re-queuing with backoff delay |
| `[GOVERNANCE_INTERCEPT]` | **WARNING** | `queue_tasks.py` | PII detected and redacted in step output |
| `[DLQ-SENT]` | ERROR | `dlq.py` | Final failure; payload pushed to `asyncflow:dlq` |
| `[REPLAY]` | INFO | `dlq.py`, `main.py` | Admin replay triggered; jobs re-enqueued |

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

## 10. Phase 2 — MCP Governance Interceptor

### What Is It?

The **MCP Governance Interceptor** is a middleware layer — modelled after a SAM (Sensitive-data Access Management) layer — that sits between `_run_llm_step()` output and downstream step chaining inside `process_workflow`.

For every `EXTRACT_JSON` and `CUSTOM_PROMPT` step, the raw LLM JSON is:
1. Parsed and validated against an `EnterpriseToolPayload` Pydantic schema.
2. Recursively scanned for PII keys at any nesting depth.
3. Sensitive values replaced with `[REDACTED_BY_POLICY]` in-place.
4. Written as one JSONL record to `data/audit.jsonl` (before/after snapshots).
5. The sanitized version replaces the raw output — the next step **never sees PII**.

### Data Flow

```
process_workflow()
    │
    ├── _run_llm_step()  ──▶  raw_output (JSON string)
    │
    ├── [EXTRACT_JSON or CUSTOM_PROMPT + GOVERNANCE_ENABLED=true]
    │       ▼
    │   GovernanceInterceptor.parse_and_sanitize(raw_output, step_id)
    │       ├── json.loads()            ──▶  GovernanceError on JSONDecodeError
    │       ├── EnterpriseToolPayload() ──▶  GovernanceError on ValidationError
    │       └── _redact_recursive()     ──▶  (sanitized_dict, redacted_keys)
    │
    ├── [redacted_keys non-empty]  ──▶  logger.warning("[GOVERNANCE_INTERCEPT] ...")
    │
    ├── AuditLogger.log_event()    ──▶  data/audit.jsonl  (append-only)
    │
    └── output = json.dumps(sanitized_dict)  ──▶  next step / final_output
```

### PII Field Registry

`GovernanceInterceptor` detects the following field names **at any nesting depth**, case-insensitively:

| Category | Fields |
|---|---|
| Tax / Identity | `tax_id`, `ssn`, `social_security_number`, `national_id`, `passport_number`, `driver_license` |
| Personnel | `employee_name`, `full_name`, `date_of_birth`, `dob` |
| Contact | `email`, `email_address`, `phone`, `phone_number`, `mobile` |
| Financial | `account_number`, `bank_account`, `credit_card`, `salary`, `compensation` |
| Network | `ip_address` |

To add a new field: extend `PII_FIELD_NAMES` in `interceptor.py` and declare it in `EnterpriseToolPayload`. No other code changes required.

### Audit Log Schema (`data/audit.jsonl`)

One JSON object per line, UTF-8. The file is **append-only** and **gitignored** — it must be protected by filesystem ACLs in production.

```json
{
  "timestamp": "2026-09-23T12:05:31.842193+00:00",
  "tool_name": "step_extract_employee",
  "redacted_keys": ["ssn", "employee.tax_id"],
  "original_payload": {
    "ssn": "123-45-6789",
    "employee": { "tax_id": "TX-001" },
    "department": "Engineering"
  },
  "sanitized_payload": {
    "ssn": "[REDACTED_BY_POLICY]",
    "employee": { "tax_id": "[REDACTED_BY_POLICY]" },
    "department": "Engineering"
  }
}
```

> **Security note:** `original_payload` contains raw PII — it is the compliance record. Never commit `data/audit.jsonl` to source control.

### Fault Integration

`GovernanceError(RuntimeError)` is caught by the existing `except (..., Exception)` handler in `process_workflow` and re-raised — activating the identical RQ retry + DLQ routing used for `LLMConnectionError` and `LLMMalformedResponseError`. **No special-casing is needed.**

| Failure Scenario | GovernanceError Trigger | RQ Behaviour |
|---|---|---|
| LLM output is not valid JSON | `json.loads()` fails | Retry → DLQ |
| Pydantic schema validation error | `EnterpriseToolPayload()` fails | Retry → DLQ |
| `data/audit.jsonl` write fails | `AuditLogger._write_event()` fails | Retry → DLQ |

### Disabling the Interceptor

Set in `.env` for local development — no code changes required:

```ini
GOVERNANCE_ENABLED=false
```

When disabled: step output passes through unmodified, no audit write occurs, and the `[GOVERNANCE_INTERCEPT]` tag is never emitted. All other fault machinery (retry, DLQ) is unaffected.

---

## 11. Fault Model (Complete)

### Step-Level

| Exception | Step Retry | Job-Level Behaviour |
|---|---|---|
| `LLMConnectionError` | No | Step FAILED, remaining SKIPPED, exception **re-raised** → RQ retry/DLQ |
| `LLMTimeoutError` | Yes (if `retry_on_failure=True`) | Step retried once; on exhaustion, re-raised → RQ retry/DLQ |
| `LLMMalformedResponseError` | Yes (if `retry_on_failure=True`) | Same as timeout |
| `GovernanceError` | Yes (if `retry_on_failure=True`) | Un-parseable JSON or audit write failure; same retry path as malformed response |
| Any other `Exception` | Yes (if `retry_on_failure=True`) | Same retry behaviour |

### Job-Level (RQ)

| Event | Trigger | Response |
|---|---|---|
| Job exception | `process_workflow` raises | RQ checks `retries_left` |
| `retries_left > 0` | — | Re-enqueue with exponential backoff |
| `retries_left == 0` | — | `route_to_dlq()` pushes to `asyncflow:dlq` |
| DLQ push failure | Redis write error | `CRITICAL`-level log; exception detail preserved |

---

## 12. Hardware Constraints

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

## 13. Environment Variables

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
| `AUDIT_LOG_PATH` | `data/audit.jsonl` | Path for the append-only JSONL governance audit log |
| `GOVERNANCE_ENABLED` | `true` | Set to `false` to bypass the PII interceptor in local dev |
| `APP_ENV` | `development` | Runtime environment tag |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `PYTHONUTF8` | (set manually) | Set to `1` on Windows to prevent cp1252 encoding errors |

---

## 14. Example End-to-End Run

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

### Governance Interception (Phase 2 — MCP)

Submit a workflow whose `EXTRACT_JSON` step returns a payload containing PII:

```powershell
# 1. Submit a governance test workflow
$body = @'
{
  "workflow_name": "governance_test",
  "input_text": "Employee John Doe, SSN 123-45-6789, Tax ID TX-001, Engineering dept.",
  "steps": [{
    "step_id": "step_extract",
    "config": {
      "task_type": "extract_json",
      "output_schema": {
        "type": "object",
        "properties": {
          "employee_name": { "type": "string" },
          "ssn":           { "type": "string" },
          "tax_id":        { "type": "string" },
          "department":    { "type": "string" }
        }
      }
    }
  }]
}
'@
Invoke-RestMethod -Method Post -Uri http://localhost:8000/workflow/submit `
  -ContentType "application/json" -Body $body

# 2. Poll status
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/workflow/<task_id>/status" `
  | ConvertTo-Json -Depth 6
```

**Expected output** — PII fields are redacted, non-sensitive fields untouched:

```json
{
  "step_id": "step_extract",
  "status": "completed",
  "output": "{\n  \"employee_name\": \"[REDACTED_BY_POLICY]\",\n  \"department\": \"Engineering\",\n  \"tax_id\": \"[REDACTED_BY_POLICY]\",\n  \"ssn\": \"[REDACTED_BY_POLICY]\"\n}"
}
```

**Worker log** — `[GOVERNANCE_INTERCEPT]` tag emitted:

```
WARNING  [GOVERNANCE_INTERCEPT] Step 'step_extract' — 3 PII field(s) redacted: ['employee_name', 'ssn', 'tax_id']
```

**Audit log** — inspect `data/audit.jsonl`:

```powershell
Get-Content data\audit.jsonl | python -m json.tool
# Shows one JSONL entry with original_payload (raw PII) and sanitized_payload
```

---

## 15. Component Status

| Component | Status | Notes |
|---|---|---|
| Pydantic V2 Schemas | ✅ Complete | Discriminated union, field validators, unique step IDs, `DlqReplayResponse` |
| FastAPI Routes | ✅ Complete | 202 submit, status polling, full RQ → WorkflowStatus mapping |
| Redis / RQ Integration | ✅ Complete | AOF persistence, SimpleWorker, fail-fast ping on startup |
| Prompt Engineering | ✅ Complete | Per-task system instruction builders, frozen `PromptPackage` dataclass |
| LLM Engine (Ollama) | ✅ Complete | `generate()`, `generate_json()`, 1 auto-retry on HTTP 5xx |
| Phase 2 — Exponential Backoff Retry | ✅ Complete | `Retry(max=3, interval=[2,4,8])`, `[RETRYING]` telemetry, fatal re-raise fix (v0.6.1) |
| Phase 2 — MCP Governance Interceptor | ✅ Complete | `GovernanceInterceptor`, 21-field PII registry, recursive redaction, `[GOVERNANCE_INTERCEPT]` telemetry, `GOVERNANCE_ENABLED` kill-switch |
| Phase 2 — Governance Audit Logger | ✅ Complete | `AuditLogger`, append-only JSONL at `data/audit.jsonl`, `GovernanceError` on write failure |
| Phase 3 — Dead-Letter Queue | ✅ Complete | `route_to_dlq` on_failure callback, `GET /dlq` inspection with pagination |
| Phase 3 — DLQ Replay | ✅ Complete | `POST /dlq/replay`, atomic RPOP drain, fresh UUID + retry reset, enqueue-failure recovery |
| Windows Compatibility | ✅ Complete | `SimpleWorker`, `PYTHONUTF8=1`, ASCII-only log strings |
| Docker Infrastructure | ✅ Complete | Redis 7-Alpine, AOF, RedisInsight on `--profile debug` |

---

*Built with [Google Antigravity IDE](https://antigravity.google.dev) · AsyncFlow Engine v1.0.0*
