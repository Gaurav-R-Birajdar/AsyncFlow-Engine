"""
AsyncFlow Engine — LLM Engine (Stub).

Responsibility:
  Provide a clean, typed interface to the local Ollama/Llama 3.1 instance.

Status: STUB — Implement after schemas and queue are locked in.

Why isolate this?
  Separating LLM communication from queue logic means we can swap Ollama
  for any other provider (OpenAI-compatible, vLLM, etc.) by only changing
  this file.
"""

from app.core.config import settings


class LLMEngine:
    """
    Thin wrapper around the Ollama HTTP API.

    Methods will be filled in during the LLM integration phase.
    """

    def __init__(self) -> None:
        self.base_url = settings.OLLAMA_BASE_URL
        self.model = settings.OLLAMA_MODEL
        self.timeout = settings.OLLAMA_REQUEST_TIMEOUT

    def generate(self, prompt: str, **kwargs) -> str:  # type: ignore[return]
        """
        Send a prompt to the local LLM and return the raw completion text.

        Args:
            prompt: The fully-rendered prompt string.
            **kwargs: Optional overrides (temperature, max_tokens, etc.).

        Returns:
            The LLM's text completion.

        Raises:
            LLMConnectionError: If Ollama is unreachable.
            LLMTimeoutError: If the response exceeds ``OLLAMA_REQUEST_TIMEOUT``.
        """
        # TODO: Implement HTTP call to Ollama /api/generate
        raise NotImplementedError("LLM engine is not yet implemented.")
