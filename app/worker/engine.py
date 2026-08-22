"""
AsyncFlow Engine — LLM Engine (Phase 2 — Production).

Responsibility:
  Provide a clean, typed, fault-tolerant synchronous interface to the local
  Ollama/Llama 3.1 instance.  This class is the ONLY place in the codebase
  that speaks the Ollama HTTP protocol.

Architecture:
  queue_tasks.py  →  prompts.py (PromptPackage)  →  engine.py (HTTP)  →  Ollama
                                                                           ↓
                                                                     Llama 3.1 (RTX 5060)

Hardware constraints (RTX 5060, 8 GB VRAM):
  - Llama 3.1 8B Q4_K_M occupies ~4.7 GB of VRAM.
  - Remaining ~3.3 GB is KV-cache budget.
  - ``num_predict`` is hard-capped at ``settings.OLLAMA_MAX_TOKENS`` (default 4096)
    to prevent the KV-cache from spilling to system RAM (which would trigger
    an Out-of-Memory crash or severe latency degradation).
  - ``stream=False`` ensures the worker blocks until the full response is ready,
    enforcing sequential inference — one context window at a time on the GPU.

Swappability:
  This file can be replaced with an OpenAI-compatible client, a vLLM adapter,
  or any other provider by changing only this module.  The rest of the codebase
  remains identical.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom Exceptions
# ---------------------------------------------------------------------------


class LLMConnectionError(RuntimeError):
    """
    Raised when Ollama is unreachable at the configured ``OLLAMA_BASE_URL``.

    Common causes:
      - Ollama service not started (run: ``ollama serve``)
      - Wrong port in ``.env`` (default: 11434)
      - Firewall blocking loopback connections
    """


class LLMTimeoutError(RuntimeError):
    """
    Raised when Ollama does not return a complete response within
    ``OLLAMA_REQUEST_TIMEOUT`` seconds.

    Common causes:
      - Model is still loading (first request after cold start can be slow)
      - Input text too long for the available KV-cache budget
      - ``num_predict`` set too high relative to available VRAM
    """


class LLMMalformedResponseError(ValueError):
    """
    Raised by ``generate_json()`` when the model output is not valid JSON,
    even after Ollama's ``format="json"`` constraint is applied.

    Why does this still happen?
      ``format="json"`` forces the tokeniser to sample only tokens that keep
      the output valid JSON *syntactically*, but very rarely the model will
      produce a structurally valid but semantically empty object ``{}``
      instead of the expected schema-conformant data.

    The caller (``queue_tasks.py``) surfaces this as a step FAILED with
    ``retry_on_failure=True`` triggering one automatic retry.
    """


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class LLMEngine:
    """
    Synchronous, blocking adapter to the Ollama ``/api/generate`` endpoint.

    Instantiated fresh per-step inside the RQ worker process.  There is no
    connection pool to manage because:
      1. Only one inference runs at a time (sequential by design).
      2. Ollama itself is stateless between requests.

    Usage::

        engine = LLMEngine()
        text = engine.generate(prompt="...", system="...")
        data = engine.generate_json(prompt="...", system="...", schema={...})
    """

    def __init__(self) -> None:
        self.base_url: str = settings.OLLAMA_BASE_URL.rstrip("/")
        self.model: str = settings.OLLAMA_MODEL
        self.timeout: int = settings.OLLAMA_REQUEST_TIMEOUT
        self._generate_url: str = f"{self.base_url}/api/generate"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(
        self,
        prompt: str,
        system: str = "",
        options: dict[str, Any] | None = None,
    ) -> str:
        """
        Send a prompt to the local LLM and return the raw completion text.

        Args:
            prompt:  The user-turn text (fully rendered by ``prompts.py``).
            system:  The system instruction string.  Passed to Ollama's
                     dedicated ``system`` field rather than prepended to
                     ``prompt`` — Llama 3.1 uses the system role natively.
            options: Optional Ollama model parameter overrides.
                     Common keys: ``temperature``, ``num_predict``, ``top_p``.

        Returns:
            The model's text completion with leading/trailing whitespace stripped.

        Raises:
            LLMConnectionError:  Ollama unreachable.
            LLMTimeoutError:     Response exceeded ``OLLAMA_REQUEST_TIMEOUT``.
        """
        payload = self._build_payload(
            prompt=prompt,
            system=system,
            options=options,
            use_json_mode=False,
        )
        raw = self._post(payload)
        return raw.get("response", "").strip()

    def generate_json(
        self,
        prompt: str,
        system: str = "",
        schema: dict[str, Any] | None = None,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Send a prompt to the LLM and return a parsed JSON object.

        Enables Ollama's ``format="json"`` parameter, which constrains the
        tokeniser's output to syntactically valid JSON at every sampling step.
        This is the core B2B value for ``EXTRACT_JSON`` tasks.

        Args:
            prompt:  The user-turn text.
            system:  The system instruction (should include the schema reminder).
            schema:  The JSON Schema dict from ``ExtractJsonConfig.output_schema``.
                     Passed for logging/debugging; the schema is already embedded
                     in the ``system`` instruction by ``prompts.py``.
            options: Optional Ollama model parameter overrides.

        Returns:
            A parsed ``dict`` from the model's JSON output.

        Raises:
            LLMConnectionError:       Ollama unreachable.
            LLMTimeoutError:          Response timed out.
            LLMMalformedResponseError: Model output is not valid JSON despite
                                       ``format="json"`` being set.
        """
        payload = self._build_payload(
            prompt=prompt,
            system=system,
            options=options,
            use_json_mode=True,
        )
        raw = self._post(payload)
        response_text = raw.get("response", "").strip()

        if schema:
            logger.debug(
                "generate_json: schema fields=%s",
                list(schema.get("properties", {}).keys()),
            )

        try:
            return json.loads(response_text)
        except json.JSONDecodeError as exc:
            # Preserve the raw text in the exception for debugging
            raise LLMMalformedResponseError(
                f"LLM output is not valid JSON even with format='json' enabled. "
                f"Raw output (first 300 chars): {response_text[:300]!r}"
            ) from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_payload(
        self,
        prompt: str,
        system: str,
        options: dict[str, Any] | None,
        use_json_mode: bool,
    ) -> dict[str, Any]:
        """
        Assemble the ``POST /api/generate`` request body.

        Design decisions:
          - ``stream=False``: Blocks until the full response arrives.  Required
            for the synchronous RQ worker model — there is no event loop to
            handle streaming chunks.
          - ``num_predict`` clamped to ``OLLAMA_MAX_TOKENS``: Hard VRAM budget
            guard.  Ollama will generate *up to* this many tokens; the model
            may stop earlier if it hits EOS naturally.
          - ``format="json"`` is only set when ``use_json_mode=True`` to avoid
            forcing non-extraction tasks into JSON-constrained sampling.
        """
        merged_options: dict[str, Any] = {
            "temperature": settings.OLLAMA_TEMPERATURE_DEFAULT,
            "num_predict": settings.OLLAMA_MAX_TOKENS,
        }
        if options:
            # Caller-supplied options override defaults, but num_predict is capped
            merged_options.update(options)
            merged_options["num_predict"] = min(
                int(merged_options.get("num_predict", settings.OLLAMA_MAX_TOKENS)),
                settings.OLLAMA_MAX_TOKENS,
            )

        payload: dict[str, Any] = {
            "model": self.model,
            "prompt": prompt,
            "system": system,
            "stream": False,
            "options": merged_options,
        }

        if use_json_mode:
            payload["format"] = "json"

        return payload

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the HTTP POST to Ollama and return the parsed response dict.

        Retry policy:
          One automatic retry on transient 5xx errors.  This covers the common
          case of Ollama briefly returning 500 while loading the model weights
          on the first request after a cold start.

        Args:
            payload: Fully-assembled Ollama API request body.

        Returns:
            Parsed JSON response from Ollama.

        Raises:
            LLMConnectionError: ``ConnectError`` — Ollama not running.
            LLMTimeoutError:    ``TimeoutException`` — response too slow.
            RuntimeError:       Non-recoverable HTTP error after one retry.
        """
        logger.debug(
            "LLMEngine POST -> %s | model=%s | json_mode=%s | temperature=%s",
            self._generate_url,
            payload["model"],
            "format" in payload,
            payload.get("options", {}).get("temperature"),
        )

        attempt = 0
        max_attempts = 2

        while attempt < max_attempts:
            attempt += 1
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    response = client.post(self._generate_url, json=payload)

                if response.status_code == 200:
                    data: dict[str, Any] = response.json()
                    logger.debug(
                        "LLMEngine <- %s | done=%s | eval_count=%s tokens",
                        response.status_code,
                        data.get("done"),
                        data.get("eval_count", "?"),
                    )
                    return data

                # Transient server-side error — retry once
                if response.status_code >= 500 and attempt < max_attempts:
                    logger.warning(
                        "LLMEngine: Ollama returned HTTP %s on attempt %d/%d. Retrying…",
                        response.status_code,
                        attempt,
                        max_attempts,
                    )
                    continue

                # Non-recoverable client or server error
                raise RuntimeError(
                    f"Ollama API returned HTTP {response.status_code}: {response.text[:200]}"
                )

            except httpx.ConnectError as exc:
                raise LLMConnectionError(
                    f"Cannot connect to Ollama at '{self._generate_url}'. "
                    f"Ensure 'ollama serve' is running and OLLAMA_BASE_URL is correct. "
                    f"Detail: {exc}"
                ) from exc

            except httpx.TimeoutException as exc:
                raise LLMTimeoutError(
                    f"Ollama request timed out after {self.timeout}s. "
                    f"Consider increasing OLLAMA_REQUEST_TIMEOUT or reducing input size. "
                    f"Detail: {exc}"
                ) from exc

        # Should never reach here — loop always returns or raises
        raise RuntimeError("LLMEngine: exhausted retry attempts without a result.")
