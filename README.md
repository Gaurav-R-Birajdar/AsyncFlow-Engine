# AsyncFlow Engine

> A high-throughput, async multi-step LLM workflow orchestration engine built with **FastAPI**, **Redis Queue (RQ)**, and **Ollama (Llama 3.1)**.

---

## Architecture

```
Client → FastAPI (/workflow/submit)
           ↓  validates payload via Pydantic schemas
         Redis Queue (RQ)
           ↓  worker picks up job
         LLM Engine (Ollama / Llama 3.1)
           ↓  executes each step sequentially
         Redis (stores results)
           ↑
Client → FastAPI (/workflow/{task_id}/status)
```

## Directory Structure

```
app/
├── main.py           # FastAPI app & routing
├── api/routes.py     # Endpoint handlers
├── core/
│   ├── config.py     # Environment-based settings
│   └── schemas.py    # Pydantic models (source of truth)
└── worker/
    ├── engine.py     # Ollama LLM interface
    └── queue_tasks.py # RQ worker functions
```

## Quick Start

```bash
# 1. Clone and create virtualenv
python -m venv .venv && .venv\Scripts\activate  # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start Redis
docker-compose up -d

# 4. Run the API server
uvicorn app.main:app --reload

# 5. Run the RQ worker (separate terminal)
rq worker asyncflow_default

# 6. Open the API docs
http://localhost:8000/docs
```

## Supported Workflow Steps

| Task Type       | Description                              |
|-----------------|------------------------------------------|
| `summarize`     | Condense input text to N words           |
| `translate`     | Translate to a target language (BCP-47)  |
| `extract_json`  | Extract structured data per a JSON schema|
| `classify`      | Assign one or more labels to the text    |
| `sentiment`     | Analyse sentiment at doc or sentence level|
| `custom_prompt` | Arbitrary Jinja2 prompt template         |

## Example Payload

```json
{
  "workflow_name": "summarise-translate-extract",
  "input_text": "The quarterly earnings report shows record revenue of $4.2B...",
  "steps": [
    {
      "step_id": "step_summarize",
      "config": { "task_type": "summarize", "max_words": 100, "style": "bullet points" }
    },
    {
      "step_id": "step_translate",
      "config": { "task_type": "translate", "target_language": "fr" }
    },
    {
      "step_id": "step_extract",
      "config": {
        "task_type": "extract_json",
        "output_schema": {
          "type": "object",
          "properties": { "summary_fr": { "type": "string" } }
        }
      }
    }
  ]
}
```

## Development Status

| Component       | Status        |
|-----------------|---------------|
| Pydantic Schemas| ✅ Complete   |
| FastAPI Routes  | 🟡 Stub       |
| Redis / RQ      | 🟡 Stub       |
| Ollama Engine   | 🔴 Pending    |

---

*Built with the AsyncFlow Engine — Google Antigravity IDE*
