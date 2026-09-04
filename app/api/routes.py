"""
AsyncFlow Engine — API Routes (v0.6.0: DLQ Replay Mechanism).

Endpoints:
  POST /workflow/submit           — Validate payload, enqueue to RQ (with Retry + DLQ), return job_id.
  GET  /workflow/{task_id}/status — Fetch live job status and results from RQ/Redis.
  GET  /workflow/dlq              — Inspect all permanently failed payloads from asyncflow:dlq (paginated).
  GET  /dlq                       — Top-level alias with pagination (mounted directly on app in main.py).
  POST /dlq/replay                — Phase 3: drain DLQ and re-enqueue all failed jobs as fresh RQ tasks.

Dependency pattern:
  Both endpoints access ``request.app.state`` for Redis/RQ objects that were
  initialised in the FastAPI lifespan.  This avoids global singletons and
  makes the routes trivially testable by injecting a mock app state.

RQ ↔ AsyncFlow status mapping:
  RQ "queued"   → WorkflowStatus.QUEUED
  RQ "started"  → WorkflowStatus.RUNNING
  RQ "finished" → WorkflowStatus.COMPLETED
  RQ "failed"   → WorkflowStatus.FAILED
  RQ "stopped"  → WorkflowStatus.CANCELLED
  RQ "deferred" → WorkflowStatus.QUEUED
  RQ "scheduled"→ WorkflowStatus.QUEUED

Retry + DLQ wiring (v0.5.0):
  ``submit_workflow`` now passes ``retry=Retry(max=3, interval=[2,4,8])`` and
  ``on_failure=route_to_dlq`` to every ``queue.enqueue()`` call.  This means:
    - RQ automatically retries failed jobs up to 3 times with exponential delays.
    - After the final failure, ``route_to_dlq`` pushes the payload + traceback
      to the ``asyncflow:dlq`` Redis list.
  The ``GET /dlq`` endpoint surfaces all DLQ entries for manual inspection.
"""

import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request, status
from rq.exceptions import NoSuchJobError
from rq.job import Job, JobStatus, Retry

from app.core.config import settings
from app.core.schemas import (
    StepResult,
    StepStatus,
    WorkflowStatus,
    WorkflowStatusResponse,
    WorkflowSubmitRequest,
    WorkflowSubmitResponse,
)
from app.worker.dlq import route_to_dlq
from app.worker.queue_tasks import process_workflow

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# RQ → WorkflowStatus translation table
# ---------------------------------------------------------------------------

_RQ_STATUS_MAP: dict[JobStatus, WorkflowStatus] = {
    JobStatus.QUEUED:    WorkflowStatus.QUEUED,
    JobStatus.CREATED:   WorkflowStatus.QUEUED,    # RQ 2.x: job created but not yet queued
    JobStatus.STARTED:   WorkflowStatus.RUNNING,
    JobStatus.FINISHED:  WorkflowStatus.COMPLETED,
    JobStatus.FAILED:    WorkflowStatus.FAILED,
    JobStatus.CANCELED:  WorkflowStatus.CANCELLED, # RQ 2.x: single-L American spelling
    JobStatus.DEFERRED:  WorkflowStatus.QUEUED,
    JobStatus.SCHEDULED: WorkflowStatus.QUEUED,
}


def _map_rq_status(rq_status: JobStatus) -> WorkflowStatus:
    """
    Translate an RQ ``JobStatus`` to our domain ``WorkflowStatus``.

    Falls back to QUEUED for any unknown future RQ states to avoid
    5xx responses on a status check.
    """
    return _RQ_STATUS_MAP.get(rq_status, WorkflowStatus.QUEUED)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetch_job(task_id: str, redis_conn) -> Job:
    """
    Fetch a job from Redis by ID.

    Raises:
        HTTPException(404): If the job does not exist in Redis.
        HTTPException(503): If Redis is unreachable during the fetch.
    """
    try:
        return Job.fetch(task_id, connection=redis_conn)
    except NoSuchJobError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task '{task_id}' not found. It may have expired or never been submitted.",
        )
    except Exception as exc:
        logger.error("Redis error while fetching job '%s': %s", task_id, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Queue service is temporarily unavailable. Retry shortly.",
        )


def _build_status_response(job: Job, workflow_name: str) -> WorkflowStatusResponse:
    """
    Construct a ``WorkflowStatusResponse`` from a live RQ ``Job`` object.

    Why parse ``job.result`` as a list of StepResult dicts?
      The worker returns a plain dict (not a Pydantic model) for pickle safety.
      We reconstruct the typed models here at the API boundary.
    """
    rq_status = job.get_status()
    workflow_status = _map_rq_status(rq_status)

    # Parse step results only when the job has finished
    step_results: list[StepResult] = []
    final_output: str | None = None
    error_detail: str | None = None

    if workflow_status == WorkflowStatus.COMPLETED and job.result:
        raw: dict = job.result
        step_results = [StepResult(**sr) for sr in raw.get("step_results", [])]
        final_output = raw.get("final_output")

    elif workflow_status == WorkflowStatus.FAILED:
        # RQ stores the exception as a string in job.exc_info
        error_detail = str(job.exc_info) if job.exc_info else "Unknown worker error."

    return WorkflowStatusResponse(
        task_id=job.id,
        workflow_name=workflow_name,
        status=workflow_status,
        step_results=step_results,
        final_output=final_output,
        submitted_at=datetime.fromtimestamp(job.enqueued_at.timestamp(), tz=timezone.utc)
        if job.enqueued_at
        else None,
        completed_at=datetime.fromtimestamp(job.ended_at.timestamp(), tz=timezone.utc)
        if job.ended_at
        else None,
        error=error_detail,
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/submit",
    response_model=WorkflowSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a multi-step LLM workflow",
)
async def submit_workflow(
    payload: WorkflowSubmitRequest,
    request: Request,
) -> WorkflowSubmitResponse:
    """
    Validate the incoming workflow payload and enqueue it for async processing.

    - Pydantic validates the payload (discriminated union, unique step IDs, etc.)
    - The validated model is serialised to a plain dict for RQ pickle safety.
    - RQ assigns a UUID job ID, which becomes the client's ``task_id``.

    Returns 202 ACCEPTED immediately — the workflow runs in the background.
    Poll ``/workflow/{task_id}/status`` to track progress.
    """
    queue = request.app.state.queue

    # Build retry policy: exponential backoff, values read from settings.
    # Retry(max=3, interval=[2, 4, 8]) — RQ will wait 2s, then 4s, then 8s
    # between consecutive retry attempts.  After 3 retries, on_failure fires.
    retry_policy = Retry(
        max=settings.RQ_RETRY_MAX,
        interval=settings.RQ_RETRY_INTERVALS,
    )

    try:
        # Serialise to a plain dict — Pydantic V2 model_dump with json mode
        # ensures datetimes, enums, etc. are all JSON-serialisable primitives.
        job = queue.enqueue(
            process_workflow,
            payload.model_dump(mode="json"),
            job_id=None,           # Let RQ generate a UUID job ID
            retry=retry_policy,    # Exponential backoff: 2s, 4s, 8s
            on_failure=route_to_dlq,  # Push to asyncflow:dlq on final failure
        )
        # Telemetry: [QUEUED] — job accepted into the Redis queue.
        logger.info(
            "[QUEUED] Workflow '%s' enqueued as job '%s' with %d step(s). "
            "Retry policy: max=%d, intervals=%s.",
            payload.workflow_name,
            job.id,
            len(payload.steps),
            settings.RQ_RETRY_MAX,
            settings.RQ_RETRY_INTERVALS,
        )
    except Exception as exc:
        logger.error("Failed to enqueue workflow '%s': %s", payload.workflow_name, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not connect to the job queue. Ensure Redis is running.",
        )

    return WorkflowSubmitResponse(
        task_id=job.id,
        status=WorkflowStatus.QUEUED,
        message=(
            f"Workflow '{payload.workflow_name}' accepted — "
            f"{len(payload.steps)} step(s) queued. "
            f"Poll /workflow/{job.id}/status for updates."
        ),
    )


@router.get(
    "/{task_id}/status",
    response_model=WorkflowStatusResponse,
    summary="Poll the status of an enqueued workflow",
)
async def get_workflow_status(
    task_id: str,
    request: Request,
) -> WorkflowStatusResponse:
    """
    Return the current execution status and results for a workflow job.

    Status transitions:
      QUEUED → RUNNING → COMPLETED | FAILED | CANCELLED

    The ``step_results`` array is populated only when status is COMPLETED.
    The ``error`` field is populated when status is FAILED.

    Why poll instead of WebSocket?
      Polling keeps the API stateless and horizontally scalable.
      A WebSocket upgrade path is planned for V0.3+.
    """
    redis_conn = request.app.state.redis_conn
    job = _fetch_job(task_id, redis_conn)

    # RQ stores positional args in job.args (tuple), not job.kwargs.
    # queue.enqueue(process_workflow, payload_dict) → job.args = (payload_dict,)
    request_dict: dict = (job.args[0] if job.args else None) or job.kwargs.get("request_dict", {})
    workflow_name: str = request_dict.get("workflow_name", "unknown")

    return _build_status_response(job, workflow_name)


# ---------------------------------------------------------------------------
# Dead-Letter Queue inspection endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/dlq",
    summary="Inspect permanently failed workflow payloads",
    tags=["Dead-Letter Queue"],
)
async def get_dlq_entries(
    request: Request,
    limit: int = Query(
        default=None,
        ge=1,
        le=1000,
        description="Max DLQ entries to return (newest-first). Defaults to DLQ_MAX_ENTRIES setting.",
    ),
) -> list[dict]:
    """
    Fetch all entries from the ``asyncflow:dlq`` Redis list.

    Each entry represents a workflow job that exhausted all RQ retry attempts
    and was routed to the Dead-Letter Queue by ``route_to_dlq``.

    Entry schema:
    ::

        {
          "job_id":        str,
          "workflow_name": str | null,
          "enqueued_at":   str (ISO-8601) | null,
          "failed_at":     str (ISO-8601),
          "attempt":       int,
          "traceback":     str,
          "payload":       dict
        }

    Returns:
        A list of DLQ entry dicts, ordered newest-first (LPUSH head is index 0).
        Returns an empty list if no entries exist.

    Raises:
        HTTPException(503): If Redis is unreachable during the fetch.
    """
    redis_conn = request.app.state.redis_conn
    dlq_key: str = settings.DLQ_REDIS_KEY
    cap: int = limit if limit is not None else settings.DLQ_MAX_ENTRIES

    try:
        # LRANGE 0 (cap-1) returns at most `cap` entries without blocking.
        raw_entries: list[bytes] = redis_conn.lrange(dlq_key, 0, cap - 1)
    except Exception as exc:
        logger.error("Redis error while fetching DLQ key '%s': %s", dlq_key, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not read from the Dead-Letter Queue. Ensure Redis is running.",
        )

    parsed: list[dict] = []
    for raw in raw_entries:
        try:
            parsed.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError) as exc:
            # Malformed entry — log and surface as a placeholder rather than crashing.
            logger.warning("DLQ entry could not be decoded: %s | raw: %r", exc, raw)
            parsed.append({"error": "malformed DLQ entry", "raw": str(raw)})

    logger.info(
        "GET /dlq — returned %d DLQ entr%s from key '%s'.",
        len(parsed),
        "y" if len(parsed) == 1 else "ies",
        dlq_key,
    )
    return parsed
