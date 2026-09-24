"""
AsyncFlow Engine — RQ Background Worker Tasks (v1.0.0: MCP Governance Interceptor).

This module defines the functions that RQ workers deserialise from Redis
and execute in a separate process.  Functions here MUST be:
  - importable at the top level (no closures, no lambdas)
  - serialisable by pickle (all arguments must be plain dicts / primitives)
  - self-contained (bring in their own logger, not shared state)

Phase 2 — Real LLM Mode (v0.4.x+):
  The Ollama engine is fully connected.  ``process_workflow`` calls
  ``prompts.build_prompt()`` to construct a task-specific system instruction,
  then passes the resulting ``PromptPackage`` to ``LLMEngine.generate()``
  (or ``LLMEngine.generate_json()`` for EXTRACT_JSON steps).

  Sequential execution is enforced by the RQ SimpleWorker — only one
  ``process_workflow`` call runs at a time, which means only one Ollama
  inference runs at a time.  This is intentional: it prevents two context
  windows from competing for the RTX 5060's 8 GB VRAM budget.

Phase 2 — Retry + DLQ (v0.5.0):
  Job-level retry is handled by RQ's native ``Retry`` mechanism, configured
  in ``run_worker.py`` (via ``queue.enqueue(..., retry=Retry(...))``).
  If all retries are exhausted, ``dlq.route_to_dlq`` is invoked as the
  RQ ``on_failure`` callback and pushes the payload to ``asyncflow:dlq``.

  This module focuses on step-level retry (``retry_on_failure`` per step)
  and emits lifecycle telemetry at every stage:
    [QUEUED]              — logged by the submit route when the job enters the queue.
    [PROCESSING]          — logged here when process_workflow() starts executing.
    [RETRYING]            — logged here when a step is retried after failure.
    [DLQ-SENT]            — logged in dlq.route_to_dlq() after final job failure.
    [GOVERNANCE_INTERCEPT]— logged here when PII is detected and redacted (Phase 2).

Phase 2 — MCP Governance Interceptor (v1.0.0):
  For EXTRACT_JSON and CUSTOM_PROMPT steps (the only step types that produce
  structured JSON), the raw LLM output is intercepted *before* it is chained
  to the next step.  The ``GovernanceInterceptor`` evaluates the payload
  against the enterprise PII policy (see ``app/governance/interceptor.py``),
  redacts sensitive fields in-place, and then ``AuditLogger`` writes an
  append-only JSONL record to ``data/audit.jsonl``.

  If the interceptor crashes (un-parseable JSON or schema error), it raises
  ``GovernanceError`` which is caught by the existing except-all handler,
  activating the standard exponential backoff + DLQ routing.

  Bypass: set ``GOVERNANCE_ENABLED=false`` in ``.env`` to skip interception
  for local development without touching source code.

Fault model:
  Step-level (within this module):
    - ``LLMConnectionError``        → step FAILED, workflow aborted, remaining steps SKIPPED,
                                      **exception RE-RAISED** so RQ triggers job-level retry.
    - ``LLMTimeoutError``           → step FAILED (retry_on_failure re-attempts); if step
                                      retries exhausted, **exception RE-RAISED** for RQ retry.
    - ``LLMMalformedResponseError`` → same as LLMTimeoutError.
    - ``GovernanceError``           → same as LLMMalformedResponseError (re-raised for RQ retry).
    - Any other ``Exception``       → step FAILED; **exception RE-RAISED** for RQ retry.

  Job-level (RQ Retry + DLQ):
    - Job fails all 3 RQ retries       → route_to_dlq() pushes to asyncflow:dlq
    - DLQ push itself fails            → CRITICAL-level log; payload is not silently lost

v0.6.1 — Critical bug fix:
  Prior to this patch, ``process_workflow`` caught LLMConnectionError (and other
  fatal step failures), recorded the step as FAILED, and then **returned normally**
  with a result dict.  RQ interpreted a normal return as job success, bypassing
  exponential backoff retries and DLQ routing entirely.

  Fix: after recording skip entries, the function now **re-raises the causal
  exception** so RQ sees an unhandled exception and activates its Retry +
  on_failure machinery correctly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.core.schemas import (
    StepResult,
    StepStatus,
    TaskType,
    WorkflowSubmitRequest,
    WorkflowStep,
)
from app.governance.audit import AuditLogger
from app.governance.interceptor import GovernanceError, GovernanceInterceptor
from app.worker.engine import (
    LLMConnectionError,
    LLMEngine,
    LLMMalformedResponseError,
    LLMTimeoutError,
)
from app.worker.prompts import PromptPackage, build_prompt

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LLM execution helpers
# ---------------------------------------------------------------------------


def _run_llm_step(engine: LLMEngine, step: WorkflowStep, input_text: str) -> str:
    """
    Dispatch a single step through the LLM engine.

    Why separate from the retry loop in ``process_workflow``?
      Keeping the dispatch clean allows the retry loop to call this function
      multiple times without duplicating prompt construction.

    Args:
        engine:     A reused ``LLMEngine`` instance for this workflow run.
        step:       The typed ``WorkflowStep`` (config determines prompt strategy).
        input_text: Chained input from the previous step (or seed text).

    Returns:
        The LLM's output as a string.
        For EXTRACT_JSON steps, this is a ``json.dumps()``-serialised dict so
        the output can chain into the next step as text.

    Raises:
        LLMConnectionError:       Ollama unreachable — unrecoverable for this run.
        LLMTimeoutError:          Response timed out — retryable.
        LLMMalformedResponseError: JSON decode failed — retryable when strict=True.
    """
    pkg: PromptPackage = build_prompt(step, input_text)

    logger.debug(
        "Step '%s' -> task_type=%s | json_mode=%s | temperature=%s | max_tokens=%s",
        step.step_id,
        step.config.task_type,
        pkg.use_json_mode,
        pkg.temperature,
        pkg.max_tokens,
    )

    if pkg.use_json_mode:
        # EXTRACT_JSON path — returns a parsed dict, normalise to string
        schema = getattr(step.config, "output_schema", None)
        parsed: dict[str, Any] = engine.generate_json(
            prompt=pkg.user,
            system=pkg.system,
            schema=schema,
            options={"temperature": pkg.temperature, "num_predict": pkg.max_tokens},
        )
        return json.dumps(parsed, ensure_ascii=False, indent=2)
    else:
        # All other task types — plain text completion
        return engine.generate(
            prompt=pkg.user,
            system=pkg.system,
            options={"temperature": pkg.temperature, "num_predict": pkg.max_tokens},
        )


# ---------------------------------------------------------------------------
# Main worker entry point
# ---------------------------------------------------------------------------


def process_workflow(request_dict: dict[str, Any]) -> dict[str, Any]:
    """
    RQ worker entry point — execute a multi-step workflow via the local LLM.

    This function runs in a **separate worker process** spawned by ``rq worker``.
    It must not rely on any FastAPI app state, async event loops, or shared
    in-memory objects.

    Execution model:
      1. Deserialise ``request_dict`` into a typed ``WorkflowSubmitRequest``.
      2. Instantiate ``LLMEngine`` once per workflow (avoids repeated object
         construction overhead; Ollama itself is stateless between calls).
      3. For each step (in order):
         a. Record ``started_at``.
         b. Call ``_run_llm_step()`` — blocks until Ollama responds.
         c. If ``input_override`` is set, use it; otherwise chain from the
            previous step's output (or the seed ``input_text`` for step 1).
         d. Record ``finished_at`` and append a ``StepResult``.
         e. If a step raises and ``retry_on_failure=True``, attempt once more.
         f. ``LLMConnectionError`` bypasses the retry loop — it is fatal for
            the entire workflow (Ollama is down; retrying won't help).
      4. Return a serialisable dict with all ``StepResult`` objects and the
         ``final_output`` of the last successful step.

    Args:
        request_dict: Plain dict matching the ``WorkflowSubmitRequest`` schema.
                      Passed as a dict (not a Pydantic model) for pickle safety
                      across process boundaries.

    Returns:
        A dict with keys:
          - ``step_results``: list[dict] — one per step, matching StepResult schema.
          - ``final_output``: str | None — output of the last completed step.

    Raises:
        ValueError:           If ``request_dict`` fails Pydantic validation.
        LLMConnectionError:   Re-raised after step recording so RQ triggers retry/DLQ.
        LLMTimeoutError:      Re-raised after step-retry exhaustion.
        LLMMalformedResponseError: Re-raised after step-retry exhaustion.
        Exception:            Any other unhandled step error, re-raised for RQ.

    Why re-raise instead of returning a FAILED result dict?
      RQ only activates its ``Retry`` + ``on_failure`` (DLQ) machinery when the
      worker function raises an **unhandled exception**.  A normal ``return``
      — even with a ``status: FAILED`` payload — is treated as a successful job
      completion.  Returning silently was the v0.6.0 bug.
    """
    # Reconstruct typed model inside the worker — re-validates the payload
    # in case the schema changed between enqueue and execution.
    try:
        request = WorkflowSubmitRequest(**request_dict)
    except Exception as exc:
        logger.error("Failed to deserialise workflow payload: %s", exc)
        raise ValueError(f"Invalid workflow payload: {exc}") from exc

    # Telemetry: [PROCESSING] — job has been picked up from the queue.
    logger.info(
        "[PROCESSING] Worker picked up workflow '%s' — %d step(s). Job is now executing.",
        request.workflow_name,
        len(request.steps),
    )

    # One engine instance per workflow — Ollama is stateless between calls
    engine = LLMEngine()
    # One interceptor + audit logger per workflow — shared across all steps.
    # Instantiated here (not at module level) so per-run config (e.g. a test
    # override of AUDIT_LOG_PATH) is respected without restarting the worker.
    interceptor = GovernanceInterceptor()
    audit_logger = AuditLogger()
    step_results: list[dict[str, Any]] = []
    current_input: str = request.input_text  # Seed text for the first step

    for idx, step in enumerate(request.steps, start=1):
        logger.info(
            "[%d/%d] Starting step '%s' (task: %s).",
            idx,
            len(request.steps),
            step.step_id,
            step.config.task_type,
        )

        # Honour input_override — useful for injecting external context mid-chain
        step_input = step.input_override if step.input_override else current_input

        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        attempt = 0
        max_attempts = 2 if step.retry_on_failure else 1
        connection_fatal = False  # LLMConnectionError bypasses retry

        while attempt < max_attempts:
            attempt += 1
            try:
                output = _run_llm_step(engine, step, step_input)

                # ---------------------------------------------------------
                # Phase 2: MCP Governance Interceptor
                # Only EXTRACT_JSON and CUSTOM_PROMPT steps produce JSON.
                # Other task types (summarize, translate, etc.) return plain
                # text — no PII schema applies, skip interception.
                # ---------------------------------------------------------
                _json_producing_types = {TaskType.EXTRACT_JSON, TaskType.CUSTOM_PROMPT}
                if (
                    settings.GOVERNANCE_ENABLED
                    and step.config.task_type in _json_producing_types
                ):
                    original_output = output  # preserve raw for audit trail
                    sanitized_dict, redacted_keys = interceptor.parse_and_sanitize(
                        raw_json=output,
                        tool_name=step.step_id,
                    )

                    if redacted_keys:
                        # Telemetry: [GOVERNANCE_INTERCEPT] — PII was detected and redacted.
                        logger.warning(
                            "[GOVERNANCE_INTERCEPT] Step '%s' — %d PII field(s) redacted: %s",
                            step.step_id,
                            len(redacted_keys),
                            redacted_keys,
                        )

                    # Always write audit event for intercepted steps so the
                    # compliance log reflects every governance check, not just
                    # the ones that found PII.
                    audit_logger.log_event(
                        original=json.loads(original_output),
                        sanitized=sanitized_dict,
                        tool_name=step.step_id,
                        redacted_keys=redacted_keys,
                    )

                    # Replace raw LLM output with the sanitized version so the
                    # next step in the chain never sees unredacted PII.
                    output = json.dumps(sanitized_dict, ensure_ascii=False, indent=2)

                result.status = StepStatus.COMPLETED
                result.output = output
                result.finished_at = datetime.now(timezone.utc)
                result.error = None

                logger.info(
                    "[%d/%d] Step '%s' completed in %.2fs.",
                    idx,
                    len(request.steps),
                    step.step_id,
                    result.duration_seconds or 0.0,
                )
                break  # Success — exit retry loop

            except LLMConnectionError as exc:
                # Ollama is down — retrying this step won't help.
                # Record the failure and abort; the exception is stored
                # so we can re-raise it after building skip records.
                logger.error(
                    "[%d/%d] Step '%s' — LLM unreachable (fatal): %s",
                    idx,
                    len(request.steps),
                    step.step_id,
                    exc,
                )
                result.status = StepStatus.FAILED
                result.error = f"LLMConnectionError: {exc}"
                result.finished_at = datetime.now(timezone.utc)
                connection_fatal = True
                fatal_exc: BaseException = exc  # preserved for re-raise below
                break

            except (LLMTimeoutError, LLMMalformedResponseError, Exception) as exc:
                exc_label = type(exc).__name__
                if attempt < max_attempts:
                    # Telemetry: [RETRYING] — step failed but has remaining attempts.
                    logger.warning(
                        "[RETRYING] [%d/%d] Step '%s' attempt %d/%d failed (%s): %s — "
                        "retrying step immediately.",
                        idx,
                        len(request.steps),
                        step.step_id,
                        attempt,
                        max_attempts,
                        exc_label,
                        exc,
                    )
                else:
                    logger.warning(
                        "[%d/%d] Step '%s' failed on attempt %d/%d (%s): %s",
                        idx,
                        len(request.steps),
                        step.step_id,
                        attempt,
                        max_attempts,
                        exc_label,
                        exc,
                    )
                if attempt >= max_attempts:
                    result.status = StepStatus.FAILED
                    result.error = f"{exc_label}: {exc}"
                    result.finished_at = datetime.now(timezone.utc)
                    fatal_exc = exc  # preserved for re-raise below

        # Append result — model_dump(mode="json") converts datetimes → ISO strings
        step_results.append(result.model_dump(mode="json"))

        if result.status == StepStatus.FAILED:
            # Mark all remaining steps as SKIPPED and abort the chain.
            reason = "LLM unreachable" if connection_fatal else f"step '{step.step_id}' failed"
            logger.error(
                "[PROCESSING] Step '%s' failed. Skipping remaining %d step(s). Reason: %s",
                step.step_id,
                len(request.steps) - idx,
                reason,
            )
            for skipped_step in request.steps[idx:]:
                step_results.append(
                    StepResult(
                        step_id=skipped_step.step_id,
                        status=StepStatus.SKIPPED,
                        error=f"Skipped because {reason}.",
                    ).model_dump(mode="json")
                )

            # --- v0.6.1 CRITICAL FIX -------------------------------------------
            # Re-raise the causal exception so RQ sees an unhandled failure.
            # Without this, RQ treats the normal `return` below as a successful
            # job and skips exponential backoff retries and DLQ routing entirely.
            #
            # `fatal_exc` is always bound here because:
            #   - LLMConnectionError branch sets it and sets connection_fatal=True.
            #   - The except-all branch sets it when attempt >= max_attempts.
            # Both paths immediately `break` the while-loop, falling through to here.
            logger.error(
                "[PROCESSING] Workflow '%s' aborting — re-raising '%s' for RQ retry/DLQ.",
                request.workflow_name,
                type(fatal_exc).__name__,
            )
            raise fatal_exc

        # Chain output: this step's output becomes the next step's input
        current_input = result.output or current_input

    # -------------------------------------------------------------------------
    # All steps completed successfully
    # -------------------------------------------------------------------------
    final_output = next(
        (r["output"] for r in reversed(step_results) if r.get("status") == StepStatus.COMPLETED),
        None,
    )

    logger.info(
        "[PROCESSING] Workflow '%s' finished successfully — %d step(s) completed.",
        request.workflow_name,
        len(step_results),
    )

    return {
        "step_results": step_results,
        "final_output": final_output,
    }
