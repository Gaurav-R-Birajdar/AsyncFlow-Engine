"""
AsyncFlow Engine — Environment Configuration.

All external service coordinates are read from environment variables so that
no secrets are ever baked into source code (12-Factor App principle).

Phase 2 additions (v0.5.0):
  - RQ_RETRY_MAX: number of RQ-level job retries before DLQ routing.
  - RQ_RETRY_INTERVALS: list of per-retry delay seconds (exponential backoff).
  - DLQ_REDIS_KEY: Redis list name for permanently failed job payloads.

Patch (v0.5.1):
  - DLQ_MAX_ENTRIES: upper bound on entries returned by GET /dlq (default 500).
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central settings object.  Values are populated from:
      1. Environment variables (highest priority)
      2. A ``.env`` file in the project root
      3. The defaults declared below
    """

    # --- Redis / RQ ---------------------------------------------------------
    REDIS_URL: str = "redis://localhost:6379/0"
    RQ_QUEUE_NAME: str = "asyncflow_default"
    JOB_TIMEOUT: int = 600  # seconds — max allowed worker runtime per job

    # --- Retry & Dead-Letter Queue (Phase 2 — v0.5.0) -----------------------
    # Number of RQ-level job retries before routing to DLQ.
    # Delays follow exponential backoff: 2s -> 4s -> 8s (guards GPU bottlenecks).
    RQ_RETRY_MAX: int = 3
    RQ_RETRY_INTERVALS: list[int] = [2, 4, 8]
    # Redis list key where permanently failed job payloads are stored.
    DLQ_REDIS_KEY: str = "asyncflow:dlq"
    # Patch v0.5.1: max entries returned by GET /dlq to prevent large wire transfers.
    DLQ_MAX_ENTRIES: int = 500

    # --- Ollama / Local LLM -------------------------------------------------
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.1"
    OLLAMA_REQUEST_TIMEOUT: int = 120  # seconds
    OLLAMA_MAX_TOKENS: int = 4096  # Hard num_predict cap — guards RTX 5060 8 GB KV-cache budget
    OLLAMA_TEMPERATURE_DEFAULT: float = 0.3  # Fallback; per-task prompts.py overrides this

    # --- FastAPI ------------------------------------------------------------
    APP_ENV: str = "development"  # "development" | "staging" | "production"
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return a cached singleton Settings instance.

    Why lru_cache?
      Parsing and validating environment variables on every request is
      wasteful.  A cached singleton is safe here because settings are
      immutable after startup.
    """
    return Settings()


# Module-level convenience alias used across the codebase
settings: Settings = get_settings()
