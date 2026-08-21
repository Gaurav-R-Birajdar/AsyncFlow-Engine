"""
AsyncFlow Engine — RQ Background Worker Tasks (Stub).

Responsibility:
  Define the functions that RQ workers pick off the queue and execute.

Status: STUB — Queue connection and real execution logic added in Phase 2.

Architecture note:
  Each function here is a self-contained unit of work.  RQ serialises the
  function reference + arguments to Redis; a separate ``rq worker`` process
  deserialises and runs it.  This means these functions MUST be importable
  at the top level (no closures, no lambdas).
"""

from __future__ import annotations

from app.core.schemas import WorkflowSubmitRequest


def process_workflow(request_dict: dict) -> dict:
    """
    Entry point called by the RQ worker for each submitted workflow.

    Args:
        request_dict: The serialised ``WorkflowSubmitRequest`` payload.
                      Passed as a plain dict because RQ uses JSON/pickle
                      for serialisation and Pydantic models are reconstructed
                      inside the worker process.

    Returns:
        A dict matching ``WorkflowStatusResponse`` schema with final results.

    Why accept a dict instead of a Pydantic model?
      RQ pickles function arguments.  Deserialising Pydantic models across
      process boundaries can fail if the model definition changes between
      enqueue and execution.  Using primitive dicts is safer and explicit.
    """
    # Reconstruct the typed model inside the worker process for validation
    request = WorkflowSubmitRequest(**request_dict)

    # TODO: Iterate over request.steps, call LLMEngine.generate() for each,
    #       chain outputs, persist StepResult objects to Redis.
    raise NotImplementedError(
        f"Worker processing not yet implemented for workflow '{request.workflow_name}'."
    )
