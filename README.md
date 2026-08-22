# AsyncFlow Engine

> A production-ready, multi-step LLM workflow orchestration engine built with **FastAPI**, **Redis Queue (RQ)**, and a local **Ollama / Llama 3.1** inference server.

Submit a JSON payload describing a chain of NLP tasks. The engine validates it, queues it, executes each step sequentially through the local LLM, and makes the results available to poll via a REST API — all without blocking the caller.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Directory Structure](#directory-structure)
3. [Quick Start](#quick-start)
4. [API Reference](#api-reference)
5. [Workflow Lifecycle](#workflow-lifecycle)
6. [Step Types & Configuration](#step-types--configuration)
7. [Payload Chaining & input_override](#payload-chaining--input_override)
8. [Fault Model & Retry Logic](#fault-model--retry-logic)
9. [Hardware Constraints](#hardware-constraints)
10. [Environment Variables](#environment-variables)
11. [Example End-to-End Run](#example-end-to-end-run)
12. [Component Status](#component-status)

---

## Architecture

```
+-------------------------------------------------------------------------+
|                          CLIENT (curl / app)                            |
+-----------------------------------+-------------------------------------+
                                    |  POST /workflow/submit
                                    v
+-------------------------------------------------------------------------+
|                   FastAPI  (app/main.py + app/api/routes.py)            |
|                                                                         |
|  1. Pydantic V2 validates the payload (discriminated union, unique IDs) |
|  2. Serialises model -> plain dict  (pickle-safe for RQ)                |
|  3. queue.enqueue(process_workflow, payload_dict)  -> returns job_id    |
|  4. Returns HTTP 202 ACCEPTED + task_id immediately                     |
+-----------------------------------+-------------------------------------+
                                    |  enqueue
                                    v
+-------------------------------------------------------------------------+
|                     Redis  (redis:7-alpine / AOF persistence)           |
|                                                                         |
|  Queue:  asyncflow_default                                              |
|  Stores: job payload, status, result, exception info                    |
+-----------------------------------+-------------------------------------+
                                    |  dequeue
                                    v
+-------------------------------------------------------------------------+
|             RQ SimpleWorker  (run_worker.py)                            |
|             [Windows: no os.fork() -- runs in-process]                 |
|                                                                         |
|  queue_tasks.process_workflow()                                         |
|    for each step:                                                       |
|      input = input_override ?? previous_step_output ?? seed_text       |
|      prompts.build_prompt(step, input)  ->  PromptPackage              |
|      LLMEngine.generate() | .generate_json()                           |
|      on failure: retry once if retry_on_failure=True                   |
|      on LLMConnectionError: abort entire workflow immediately           |
+-----------------------------------+-------------------------------------+
                                    |  HTTP POST /api/generate
                                    v
+-------------------------------------------------------------------------+
|              Ollama  (localhost:11434)  --  Llama 3.1 8B Q4_K_M        |
|              RTX 5060  |  8 GB VRAM  |  stream=False                   |
+-----------------------------------+-------------------------------------+
                                    |  result stored in Redis
                                    v
+-------------------------------------------------------------------------+
|                CLIENT  ->  GET /workflow/{task_id}/status               |
|                Returns: step_results[], final_output, status            |
+-------------------------------------------------------------------------+
```

---

## Directory Structure

```
AsyncFlow Engine/
+-- app/
|   +-- main.py               # FastAPI app, lifespan (Redis pool), CORS
|   +-- api/
|   |   +-- routes.py         # POST /workflow/submit, GET /workflow/{id}/status
|   +-- core/
|   |   +-- config.py         # pydantic-settings -- reads .env, lru_cache singleton
|   |   +-- schemas.py        # Pydantic V2 models -- single source of truth for all JSON shapes
|   +-- worker/
|       +-- engine.py         # LLMEngine -- sole Ollama HTTP client, retry on 5xx
|       +-- prompts.py        # build_prompt() -- per-task system instruction builders
|       +-- queue_tasks.py    # process_workflow() -- RQ entry point, chaining, retry loop
+-- run_worker.py             # Windows-compatible SimpleWorker launcher
+-- docker-compose.yml        # Redis 7-Alpine + optional RedisInsight
+-- payload.json              # Ready-to-run 3-step demo payload
+-- requirements.txt          # Pinned >= floors for all dependencies
+-- .env                      # Local environment overrides (not committed)
```

---

## Quick Start

### Prerequisites

| Requirement | Version |
|---|---|
| Python | 3.10+ |
| Docker Desktop | Any recent |
| Ollama | Latest |
| Llama 3.1 model | `ollama pull llama3.1` |

### 1 — Clone and create virtualenv

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2 — Start Redis

```powershell
docker-compose up -d
```

> **Optional:** Start RedisInsight UI on port 8001:
> ```powershell
> docker-compose --profile debug up -d
> ```

### 3 — Start Ollama

```powershell
ollama serve          # separate terminal
ollama pull llama3.1  # first time only
```

### 4 — Start the API server

```powershell
$env:PYTHONUTF8 = "1"
uvicorn app.main:app --reload
# API live at http://localhost:8000
# Swagger UI at http://localhost:8000/docs
```

### 5 — Start the RQ worker

```powershell
# Separate terminal
.\venv\Scripts\Activate.ps1
$env:PYTHONUTF8 = "1"
python run_worker.py
```

> **Why `run_worker.py` instead of `rq worker`?**
> RQ's default worker calls `os.fork()`, which is POSIX-only and crashes on Windows.
> `run_worker.py` uses `rq.SimpleWorker` — the officially documented Windows workaround.

### 6 — Submit a workflow

```powershell
curl.exe -X POST http://localhost:8000/workflow/submit `
  -H "Content-Type: application/json" `
  -d "@payload.json"
```

---

## API Reference

### `POST /workflow/submit`

Validate and enqueue a multi-step workflow. Returns immediately with a `task_id`.

**Response `202 Accepted`:**
```json
{
  "task_id": "d3f1a2b4-...",
  "status": "queued",
  "message": "Workflow 'my-pipeline' accepted — 3 step(s) queued. Poll /workflow/d3f1a2b4-.../status for updates."
}
```

| Error Code | Cause |
|---|---|
| `422` | Pydantic validation failed (bad payload schema) |
| `503` | Redis unreachable — worker queue down |

---

### `GET /workflow/{task_id}/status`

Poll the live execution state. Step results are populated once status is `completed`.

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
      "started_at": "2026-08-22T10:00:00Z",
      "finished_at": "2026-08-22T10:00:12Z"
    }
  ],
  "final_output": "{ \"revenue_billion_usd\": 4.2, ... }",
  "submitted_at": "2026-08-22T09:59:58Z",
  "completed_at": "2026-08-22T10:00:30Z",
  "error": null
}
```

| Error Code | Cause |
|---|---|
| `404` | `task_id` not found or expired in Redis |
| `503` | Redis unreachable during status fetch |

---

### `GET /health`

Liveness probe. Returns `{"status": "ok"}` if the API is running.

---

## Workflow Lifecycle

```
                   +-----------+
  on submit        |  QUEUED   |  Job accepted by Redis, awaiting worker
                   +-----+-----+
                         |  worker picks up job
                         v
                   +-----------+
                   |  RUNNING  |  Steps executing sequentially
                   +-----+-----+
          +--------+-----+---------+
          v              v          v
    +----------+   +---------+  +-----------+
    |COMPLETED |   | FAILED  |  | CANCELLED |
    +----------+   +---------+  +-----------+
```

**Per-step status:**

```
PENDING -> RUNNING -> COMPLETED
                   \-> FAILED  (-> SKIPPED for all subsequent steps)
```

---

## Step Types & Configuration

All steps share these top-level fields:

| Field | Type | Default | Description |
|---|---|---|---|
| `step_id` | string | required | Unique slug (`[a-zA-Z0-9_-]`, max 64 chars) |
| `config` | object | required | Task-specific configuration (see below) |
| `input_override` | string or null | `null` | Override chained input for this step only |
| `retry_on_failure` | boolean | `false` | Retry this step once on timeout or malformed JSON |

---

### `summarize`

Condense input text to a target word count.

```json
{ "task_type": "summarize", "max_words": 60, "style": "bullet points" }
```

| Field | Type | Range | Default |
|---|---|---|---|
| `max_words` | int | 10 – 2000 | `150` |
| `style` | string | any hint | `"narrative"` |

Style hints: `"bullet points"`, `"narrative"`, `"executive"`, `"tldr"`

---

### `sentiment`

Analyse document or sentence-level sentiment.

```json
{ "task_type": "sentiment", "granularity": "document" }
```

| Field | Type | Values | Default |
|---|---|---|---|
| `granularity` | string | `"document"` or `"sentence"` | `"document"` |

Output example: `"positive | 0.92"`

---

### `extract_json`

Extract structured data conforming to a caller-provided JSON Schema.
Uses Ollama's `format="json"` constraint to force schema-conformant sampling.

```json
{
  "task_type": "extract_json",
  "strict": true,
  "output_schema": {
    "type": "object",
    "properties": {
      "revenue_billion_usd":  { "type": "number" },
      "yoy_growth_pct":       { "type": "number" },
      "operating_margin_pct": { "type": "number" },
      "buyback_million_usd":  { "type": "number" }
    },
    "required": ["revenue_billion_usd", "yoy_growth_pct", "operating_margin_pct", "buyback_million_usd"]
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `output_schema` | object | required | JSON Schema describing the expected output |
| `strict` | boolean | `true` | Fail and retry rather than return malformed JSON |

---

### `translate`

Translate to any BCP-47 language tag.

```json
{ "task_type": "translate", "target_language": "fr", "preserve_formatting": true }
```

> **Constraint:** `translate` cannot be the first step — there must be processed text to translate.
> Prepend a `summarize` or `custom_prompt` step.

---

### `classify`

Assign one or more labels from a fixed candidate set.

```json
{ "task_type": "classify", "labels": ["finance", "technology", "healthcare"], "multi_label": false }
```

| Field | Type | Default | Description |
|---|---|---|---|
| `labels` | list of strings | required | Min 2 candidates |
| `multi_label` | boolean | `false` | Allow multiple labels per input |

---

### `custom_prompt`

Arbitrary Jinja2-compatible prompt template. Use `{{ input }}` to reference the chained input.

```json
{
  "task_type": "custom_prompt",
  "prompt_template": "Rewrite the following as a formal board memo:\n\n{{ input }}",
  "temperature": 0.5,
  "max_tokens": 1024
}
```

| Field | Type | Range | Default |
|---|---|---|---|
| `prompt_template` | string | min 10 chars | required |
| `temperature` | float | 0.0 – 2.0 | `0.7` |
| `max_tokens` | int | 1 – 8192 | `512` |

---

## Payload Chaining & `input_override`

By default, each step receives the **output of the previous step** as its input. Step 1 receives the top-level `input_text`.

```
input_text
    |
    v
 step_1  --output-->  step_2  --output-->  step_3
```

Use `input_override` on any step to **break the chain** and inject arbitrary text instead. This is essential when a downstream step must operate on the original source text, not a transformed intermediate:

```
input_text -----------------------------------------> step_3 (via input_override)
    |
    v
 step_1  --output-->  step_2
```

**Practical example:** In `payload.json`, `step_extract` uses `input_override` to receive the original financial paragraph, not the `"positive | 0.87"` string produced by `step_sentiment`.

---

## Fault Model & Retry Logic

| Exception | Retryable | Effect on Workflow |
|---|---|---|
| `LLMConnectionError` | No | Abort immediately. All remaining steps -> SKIPPED. Ollama is down; retrying won't help. |
| `LLMTimeoutError` | Yes* | Step FAILED. Retried once if `retry_on_failure=True`. |
| `LLMMalformedResponseError` | Yes* | Step FAILED. Retried once if `retry_on_failure=True`. Triggered by `extract_json` with `strict=True`. |
| Any other Exception | Yes* | Same retry behaviour as above. |

*Max 2 attempts total when `retry_on_failure=True`; 1 attempt otherwise.

When a step exhausts all attempts:
- The step is marked `FAILED`.
- All subsequent steps are marked `SKIPPED`.
- The workflow `status` becomes `failed`.

---

## Hardware Constraints

This engine is tuned for a single-GPU development workstation:

| Constraint | Value | Reason |
|---|---|---|
| GPU | RTX 5060 | 8 GB VRAM |
| Model | Llama 3.1 8B Q4_K_M | ~4.7 GB VRAM footprint |
| KV-cache budget | ~3.3 GB | Remaining VRAM after model load |
| `num_predict` hard cap | `4096` tokens | Guards against KV-cache overflow to system RAM |
| `stream` mode | `False` (always) | Enforces sequential, blocking inference |
| Worker concurrency | 1 (SimpleWorker) | One GPU context window at a time |

To run on Linux with multiple workers or a larger GPU, switch `run_worker.py` to `rq.Worker` and adjust `OLLAMA_MAX_TOKENS` accordingly.

---

## Environment Variables

All variables are read from `.env` at startup. Env vars take precedence over `.env` file values.

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `RQ_QUEUE_NAME` | `asyncflow_default` | RQ queue name |
| `JOB_TIMEOUT` | `600` | Max worker runtime per job (seconds) |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Ollama server URL |
| `OLLAMA_MODEL` | `llama3.1` | Model name passed to `/api/generate` |
| `OLLAMA_REQUEST_TIMEOUT` | `120` | Per-request HTTP timeout (seconds) |
| `OLLAMA_MAX_TOKENS` | `4096` | Hard `num_predict` cap (VRAM guard) |
| `OLLAMA_TEMPERATURE_DEFAULT` | `0.3` | Fallback temperature (overridden per task type by `prompts.py`) |
| `APP_ENV` | `development` | Runtime environment tag |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `PYTHONUTF8` | (set manually) | Set to `1` on Windows to prevent cp1252 encoding errors in the console |

---

## Example End-to-End Run

The included `payload.json` runs a 3-step financial analysis pipeline:

```
input_text: "The company reported Q3 revenue of $4.2 billion, up 18% YoY..."
     |
     v
step_summarize  -->  Bullet-point summary (~60 words)
     |
     v
step_sentiment  -->  "positive | 0.92"
     |
     X  (output discarded -- input_override kicks in for next step)

input_text (original) -------------------------------------------------->
step_extract    -->  { "revenue_billion_usd": 4.2,
                       "yoy_growth_pct": 18,
                       "operating_margin_pct": 24,
                       "buyback_million_usd": 500 }
```

**1. Submit:**
```powershell
curl.exe -X POST http://localhost:8000/workflow/submit `
  -H "Content-Type: application/json" `
  -d "@payload.json"
# Response: {"task_id": "abc-123-...", "status": "queued", ...}
```

**2. Poll:**
```powershell
curl.exe http://localhost:8000/workflow/abc-123-.../status
# When done: {"status": "completed", "final_output": "{\"revenue_billion_usd\": 4.2, ...}", ...}
```

**3. Check Swagger UI:**

Navigate to `http://localhost:8000/docs` for an interactive API explorer with full schema documentation.

---

## Component Status

| Component | Status | Notes |
|---|---|---|
| Pydantic V2 Schemas | Complete | Discriminated union, field validators, unique step ID enforcement |
| FastAPI Routes | Complete | 202 submit, polling status, full RQ -> WorkflowStatus mapping |
| Redis / RQ Integration | Complete | AOF persistence, SimpleWorker, fail-fast ping on startup |
| Prompt Engineering | Complete | Per-task system instruction builders, frozen `PromptPackage` dataclass |
| LLM Engine (Ollama) | Complete | `generate()`, `generate_json()`, 1 auto-retry on HTTP 5xx |
| Fault & Retry Logic | Complete | Connection-fatal abort, per-step retry, skip propagation |
| Windows Compatibility | Complete | `SimpleWorker`, `PYTHONUTF8=1`, ASCII-only log strings |
| Docker Infrastructure | Complete | Redis 7-Alpine, AOF, RedisInsight on `--profile debug` |

---

*Built with [Google Antigravity IDE](https://antigravity.google.dev) · AsyncFlow Engine v0.8*
