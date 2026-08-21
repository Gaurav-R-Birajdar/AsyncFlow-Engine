"""
AsyncFlow Engine — Environment Configuration.

All external service coordinates are read from environment variables so that
no secrets are ever baked into source code (12-Factor App principle).
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

    # --- Ollama / Local LLM -------------------------------------------------
    OLLAMA_BASE_URL: str = "http://localhost:11434"
    OLLAMA_MODEL: str = "llama3.1"
    OLLAMA_REQUEST_TIMEOUT: int = 120  # seconds

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
