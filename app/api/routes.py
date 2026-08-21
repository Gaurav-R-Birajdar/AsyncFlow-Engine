"""
AsyncFlow Engine — API Routes.

Endpoints:
  POST /workflow/submit       — Validate and enqueue a new workflow.
  GET  /workflow/{task_id}/status — Poll the status of a running workflow.
"""

import uuid

from fastapi import APIRouter, HTTPException, status

from app.core.schemas import WorkflowSubmitRequest, WorkflowSubmitResponse, WorkflowStatusResponse

router = APIRouter()


@router.post(
    "/submit",
    response_model=WorkflowSubmitResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a multi-step LLM workflow",
)
async def submit_workflow(payload: WorkflowSubmitRequest) -> WorkflowSubmitResponse:
    """
    Validate the incoming workflow payload and enqueue it for async processing.

    Returns a ``task_id`` the client can use to poll ``/workflow/{task_id}/status``.

    Why 202 ACCEPTED?
      The request is valid but the result is not yet ready — work is offloaded
      to the RQ background worker, keeping this endpoint non-blocking.
    """
    # TODO: Push to RQ queue and persist task metadata in Redis
    task_id = str(uuid.uuid4())
    return WorkflowSubmitResponse(
        task_id=task_id,
        message=f"Workflow '{payload.workflow_name}' accepted — {len(payload.steps)} step(s) queued.",
        status="queued",
    )


@router.get(
    "/{task_id}/status",
    response_model=WorkflowStatusResponse,
    summary="Poll the status of an enqueued workflow",
)
async def get_workflow_status(task_id: str) -> WorkflowStatusResponse:
    """
    Return the current execution status and partial results for a workflow.

    Why poll instead of WebSocket?
      Polling keeps the API stateless and horizontally scalable.
      WebSocket upgrade can be added in a later iteration without breaking
      existing clients.
    """
    # TODO: Fetch real status from Redis using task_id
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Task '{task_id}' not found. Submit a workflow first.",
    )
