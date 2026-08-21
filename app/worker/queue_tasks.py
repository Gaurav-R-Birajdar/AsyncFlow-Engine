"""
AsyncFlow Engine — RQ Background Worker Tasks.

This module defines the functions that RQ workers deserialise from Redis
and execute in a separate process.  Functions here MUST be:
  - importable at the top level (no closures, no lambdas)
  - serialisable by pickle (all arguments must be plain dicts / primitives)
  - self-contained (bring in their own logger, not shared state)

Phase 1 — Simulation Mode:
  The Ollama engine is NOT yet connected.  ``process_workflow`` iterates over
  the workflow steps and sleeps for 2 seconds per step to simulate LLM latency.
  This lets us validate the full state-machine (Queued → Running → Completed/Failed)
  before introducing real GPU compute.

Phase 2 note:
  Replace the ``_simulate_step`` call with ``LLMEngine().generate(prompt)``
  and the sleep with the actual HTTP round-trip.  No other changes needed.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

from app.core.schemas import (
    StepResult,
    StepStatus,
    TaskType,
    WorkflowStep,
    WorkflowSubmitRequest,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------


_SIMULATED_LATENCY_SECONDS: float = 2.0
"""Per-step sleep duration that mimics local LLM inference time."""


def _simulate_step(step: WorkflowStep, input_text: str) -> str:
    """
    Produce a deterministic mock output for a single workflow step.

    Why task-type-specific messages?
      Realistic-looking output makes it immediately obvious which step is
      being simulated when reading logs or API responses — far easier to
      debug than a generic "step completed" string.

    Args:
        step: The validated ``WorkflowStep`` object.
        input_text: The chained input from the previous step (or seed text).

    Returns:
        A short mock string representing the step's "output".
    """
    task_type = step.config.task_type
    preview = input_text[:80].replace("\n", " ")

    _mock_outputs: dict[TaskType, str] = {
        TaskType.SUMMARIZE: (
            f"[SIMULATED SUMMARY] Key points extracted from: '{preview}...'"
        ),
        TaskType.TRANSLATE: (
            f"[SIMULATED TRANSLATION → {getattr(step.config, 'target_language', '??')}] "
            f"Translated content of: '{preview}...'"
        ),
        TaskType.EXTRACT_JSON: (
            '{{"simulated": true, "extracted_from": "' + preview[:40] + '..."}}'
        ),
        TaskType.CLASSIFY: (
            f"[SIMULATED CLASSIFICATION] Label assigned to: '{preview}...'"
        ),
        TaskType.SENTIMENT: (
            f"[SIMULATED SENTIMENT] positive (confidence: 0.87) for: '{preview}...'"
        ),
        TaskType.CUSTOM_PROMPT: (
            f"[SIMULATED CUSTOM OUTPUT] Prompt applied to: '{preview}...'"
        ),
    }

    return _mock_outputs.get(
        task_type,
        f"[SIMULATED] Unknown task type '{task_type}' — output placeholder.",
    )


# ---------------------------------------------------------------------------
# Main worker entry point
# ---------------------------------------------------------------------------


def process_workflow(request_dict: dict[str, Any]) -> dict[str, Any]:
    """
    RQ worker entry point — simulate a full multi-step workflow execution.

    This function runs in a **separate worker process** spawned by ``rq worker``.
    It must not rely on any FastAPI app state, async event loops, or shared
    in-memory objects.

    Execution model:
      1. Deserialise ``request_dict`` into a typed ``WorkflowSubmitRequest``.
      2. For each step (in order):
         a. Record ``started_at``.
         b. Sleep ``_SIMULATED_LATENCY_SECONDS`` to mimic LLM inference.
         c. If ``input_override`` is set, use it; otherwise chain from the
            previous step's output (or the seed ``input_text`` for step 1).
         d. Record ``finished_at`` and append a ``StepResult``.
         e. If a step raises an exception and ``retry_on_failure`` is True,
            attempt once more before marking the step as FAILED.
      3. Return a serialisable dict with all ``StepResult`` objects and the
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
        ValueError: If ``request_dict`` fails Pydantic validation (schema drift).
    """
    # Reconstruct the typed model inside the worker — validates the payload
    # again in case the schema was updated between enqueue and execution.
    try:
        request = WorkflowSubmitRequest(**request_dict)
    except Exception as exc:
        logger.error("Failed to deserialise workflow payload: %s", exc)
        raise ValueError(f"Invalid workflow payload: {exc}") from exc

    logger.info(
        "Worker processing workflow '%s' — %d step(s).",
        request.workflow_name,
        len(request.steps),
    )

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

        # Honour input_override — useful for injecting external context
        step_input = step.input_override if step.input_override else current_input

        result = StepResult(
            step_id=step.step_id,
            status=StepStatus.RUNNING,
            started_at=datetime.now(timezone.utc),
        )

        attempt = 0
        max_attempts = 2 if step.retry_on_failure else 1

        while attempt < max_attempts:
            attempt += 1
            try:
                # --- Simulate LLM latency -----------------------------------
                time.sleep(_SIMULATED_LATENCY_SECONDS)

                output = _simulate_step(step, step_input)
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

            except Exception as exc:
                logger.warning(
                    "[%d/%d] Step '%s' failed on attempt %d/%d: %s",
                    idx,
                    len(request.steps),
                    step.step_id,
                    attempt,
                    max_attempts,
                    exc,
                )
                if attempt >= max_attempts:
                    result.status = StepStatus.FAILED
                    result.error = str(exc)
                    result.finished_at = datetime.now(timezone.utc)

        # Append result (whether success or failure)
        # Serialise via model_dump(mode="json") to handle datetime → ISO string
        step_results.append(result.model_dump(mode="json"))

        if result.status == StepStatus.FAILED:
            # Mark all remaining steps as SKIPPED and abort the chain
            logger.error(
                "Step '%s' failed. Skipping remaining %d step(s).",
                step.step_id,
                len(request.steps) - idx,
            )
            for skipped_step in request.steps[idx:]:
                step_results.append(
                    StepResult(
                        step_id=skipped_step.step_id,
                        status=StepStatus.SKIPPED,
                        error=f"Skipped because step '{step.step_id}' failed.",
                    ).model_dump(mode="json")
                )
            break

        # Chain output: this step's output becomes the next step's input
        current_input = result.output or current_input

    final_output = next(
        (r["output"] for r in reversed(step_results) if r.get("status") == StepStatus.COMPLETED),
        None,
    )

    logger.info(
        "Workflow '%s' finished. Final status: %s.",
        request.workflow_name,
        "COMPLETED" if final_output else "FAILED",
    )

    return {
        "step_results": step_results,
        "final_output": final_output,
    }
