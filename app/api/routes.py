"""
AsyncFlow Engine — API Routes.

Endpoints:
  POST /workflow/submit           — Validate payload, enqueue to RQ, return job_id.
  GET  /workflow/{task_id}/status — Fetch live job status and results from RQ/Redis.

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
"""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, status
from rq.exceptions import NoSuchJobError
from rq.job import Job, JobStatus

from app.core.schemas import (
    StepResult,
    StepStatus,
    WorkflowStatus,
    WorkflowStatusResponse,
    WorkflowSubmitRequest,
    WorkflowSubmitResponse,
)
from app.worker.queue_tasks import process_workflow

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# RQ → WorkflowStatus translation table
# ---------------------------------------------------------------------------

_RQ_STATUS_MAP: dict[JobStatus, WorkflowStatus] = {
    JobStatus.QUEUED:    WorkflowStatus.QUEUED,
    JobStatus.STARTED:   WorkflowStatus.RUNNING,
    JobStatus.FINISHED:  WorkflowStatus.COMPLETED,
    JobStatus.FAILED:    WorkflowStatus.FAILED,
    JobStatus.STOPPED:   WorkflowStatus.CANCELLED,
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

    try:
        # Serialise to a plain dict — Pydantic V2 model_dump with json mode
        # ensures datetimes, enums, etc. are all JSON-serialisable primitives.
        job = queue.enqueue(
            process_workflow,
            payload.model_dump(mode="json"),
            job_id=None,  # Let RQ generate a UUID job ID
        )
        logger.info(
            "Workflow '%s' enqueued as job '%s' with %d step(s).",
            payload.workflow_name,
            job.id,
            len(payload.steps),
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

    # workflow_name is stored in job.kwargs (the original call arguments)
    kwargs: dict = job.kwargs or {}
    request_dict: dict = kwargs.get("request_dict", {})
    workflow_name: str = request_dict.get("workflow_name", "unknown")

    return _build_status_response(job, workflow_name)
