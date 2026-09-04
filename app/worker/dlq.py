"""
AsyncFlow Engine — Dead-Letter Queue (DLQ) Router & Replay (v0.6.0).

Responsibility:
  When RQ exhausts all retry attempts for a job, it invokes the registered
  ``on_failure`` callback.  This module provides that callback: ``route_to_dlq``.

  Phase 3 addition (v0.6.0): ``replay_dlq`` — drains all entries from the DLQ
  and re-enqueues each payload as a fresh job in the primary RQ queue.  The
  replay function is intentionally kept in the worker layer (not the route) so
  it can be tested independently of FastAPI.

DLQ contract:
  - Key:    ``asyncflow:dlq`` (settings.DLQ_REDIS_KEY)
  - Type:   Redis List (LPUSH — newest entries at head)
  - Format: One JSON string per entry containing:
      {
        "job_id":        str,          # RQ job UUID
        "workflow_name": str | None,   # from the original payload
        "enqueued_at":   ISO-8601 str, # when the job was first submitted
        "failed_at":     ISO-8601 str, # when final failure was detected
        "attempt":       int,          # total attempts made (retries + 1)
        "traceback":     str,          # full exception traceback from RQ
        "payload":       dict          # the original request_dict passed to process_workflow
      }

Why LPUSH (not RPUSH)?
  LPUSH prepends entries so that ``LRANGE asyncflow:dlq 0 -1`` returns the
  most recently failed job first — easier to triage.

Why RPOP for replay (not LPOP)?
  RPOP pops from the tail of the list, replaying jobs in chronological order
  (oldest failures first).  This is the most natural reprocessing order and
  avoids re-failing a dependency before its dependents are replayed.

Thread-safety:
  LPUSH and RPOP are both atomic in Redis — no locking needed even if multiple
  workers or admin operators trigger replay concurrently.  The worst case is two
  concurrent replays each pop disjoint entries (no double-requeue).
"""

from __future__ import annotations

import json
import logging
import traceback as tb_module
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from rq import Queue
from rq.job import Retry

from app.core.config import settings

if TYPE_CHECKING:
    from rq.job import Job

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DLQ push constant
# ---------------------------------------------------------------------------

DLQ_KEY: str = settings.DLQ_REDIS_KEY


# ---------------------------------------------------------------------------
# Public callback — registered with RQ queue.enqueue(on_failure=...)
# ---------------------------------------------------------------------------


def route_to_dlq(
    job: "Job",
    connection,
    type_: type[BaseException],
    value: BaseException,
    traceback,
) -> None:
    """
    RQ ``on_failure`` callback — route a permanently failed job to the DLQ.

    RQ calls this function after a job has exceeded its ``Retry`` limit.
    The signature is dictated by RQ internals and must match exactly.

    Args:
        job:        The failed RQ ``Job`` object.
        connection: The Redis connection used by the RQ worker.
        type_:      The exception class that caused the final failure.
        value:      The exception instance.
        traceback:  The traceback object.
    """
    failed_at: str = datetime.now(timezone.utc).isoformat()

    # Reconstruct the original workflow payload from the job args.
    # queue.enqueue(process_workflow, payload_dict) stores payload_dict at job.args[0].
    original_payload: dict[str, Any] = {}
    if job.args:
        raw = job.args[0]
        if isinstance(raw, dict):
            original_payload = raw

    workflow_name: str | None = original_payload.get("workflow_name")

    # Serialize the traceback to a plain string for JSON storage.
    tb_string: str = "".join(
        tb_module.format_exception(type_, value, traceback)
    )

    # Count total attempts = original try + number of retries consumed.
    # job.retries_left starts at RQ_RETRY_MAX and decrements; after final
    # failure it is 0.  Total attempts = RQ_RETRY_MAX + 1.
    total_attempts: int = settings.RQ_RETRY_MAX + 1

    dlq_entry: dict[str, Any] = {
        "job_id": job.id,
        "workflow_name": workflow_name,
        "enqueued_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
        "failed_at": failed_at,
        "attempt": total_attempts,
        "traceback": tb_string,
        "payload": original_payload,
    }

    try:
        connection.lpush(DLQ_KEY, json.dumps(dlq_entry, ensure_ascii=False, default=str))
        logger.error(
            "[DLQ] Job '%s' (workflow: '%s') permanently failed after %d attempt(s). "
            "Payload pushed to '%s'. Error: %s: %s",
            job.id,
            workflow_name or "unknown",
            total_attempts,
            DLQ_KEY,
            type_.__name__,
            value,
        )
    except Exception as push_exc:
        # DLQ push itself failed — log and do not swallow silently.
        logger.critical(
            "[DLQ] CRITICAL: Failed to push job '%s' to DLQ key '%s': %s. "
            "Original failure: %s: %s",
            job.id,
            DLQ_KEY,
            push_exc,
            type_.__name__,
            value,
        )


# ---------------------------------------------------------------------------
# Phase 3 — DLQ Replay
# ---------------------------------------------------------------------------


def replay_dlq(redis_conn, rq_queue: Queue) -> dict[str, Any]:
    """
    Drain the Dead-Letter Queue and re-enqueue each payload as a fresh job.

    Strategy:
      - Iteratively ``RPOP`` from the tail of ``asyncflow:dlq`` (oldest-first).
      - For each entry, extract ``payload`` and call ``queue.enqueue()`` with a
        clean ``Retry`` policy (same as the original submit path), which resets
        ``retries_left`` to ``RQ_RETRY_MAX`` so the worker treats it as new.
      - Entries that are malformed (non-JSON, missing ``payload`` key) are
        counted as skipped and their raw bytes are logged at WARNING level.
        They are permanently dropped from the DLQ rather than put back, to
        avoid an infinite poison-pill loop.

    Idempotency:
      Each RPOP is atomic in Redis.  If a concurrent replay runs simultaneously,
      each entry is popped by exactly one caller — no double-requeue risk.

    Args:
        redis_conn: A synchronous ``redis.Redis`` connection.
        rq_queue:   The live RQ ``Queue`` object bound to ``asyncflow_default``.

    Returns:
        A dict with keys:
          - ``requeued``       (int)
          - ``skipped``        (int)
          - ``total_processed``(int)
          - ``skipped_details``(list[str])
    """
    from app.worker.queue_tasks import process_workflow  # local import avoids circular dep

    dlq_key: str = settings.DLQ_REDIS_KEY
    retry_policy = Retry(
        max=settings.RQ_RETRY_MAX,
        interval=settings.RQ_RETRY_INTERVALS,
    )

    requeued: int = 0
    skipped: int = 0
    skipped_details: list[str] = []

    while True:
        # RPOP is atomic — safe under concurrent callers.
        raw: bytes | None = redis_conn.rpop(dlq_key)
        if raw is None:
            # List exhausted — replay complete.
            break

        # --- Parse DLQ entry ------------------------------------------------
        try:
            entry: dict[str, Any] = json.loads(raw)
        except (json.JSONDecodeError, TypeError) as exc:
            reason = f"JSON decode error: {exc} | raw={raw!r:.120}"
            logger.warning("[REPLAY] Skipping malformed DLQ entry: %s", reason)
            skipped += 1
            skipped_details.append(reason)
            continue

        payload: dict[str, Any] | None = entry.get("payload")
        original_job_id: str = entry.get("job_id", "<unknown>")
        workflow_name: str = entry.get("workflow_name") or "<unknown>"

        if not isinstance(payload, dict) or not payload:
            reason = (
                f"Missing or empty 'payload' in DLQ entry for job '{original_job_id}' "
                f"(workflow: '{workflow_name}')"
            )
            logger.warning("[REPLAY] %s", reason)
            skipped += 1
            skipped_details.append(reason)
            continue

        # --- Re-enqueue as a fresh job with a clean retry counter -----------
        # Passing a fresh Retry() object resets retries_left to RQ_RETRY_MAX.
        # We deliberately do NOT recycle the original job_id so that RQ
        # generates a new UUID — the old failed job may still exist in Redis
        # and re-using its ID could produce undefined behaviour.
        try:
            new_job = rq_queue.enqueue(
                process_workflow,
                payload,
                job_id=None,              # Fresh UUID — treat as a brand-new job
                retry=retry_policy,       # Resets retries_left = RQ_RETRY_MAX
                on_failure=route_to_dlq,  # Re-arm DLQ routing on subsequent failures
            )
            requeued += 1
            logger.info(
                "[REPLAY] Job '%s' (workflow: '%s') re-enqueued as new job '%s'.",
                original_job_id,
                workflow_name,
                new_job.id,
            )
        except Exception as enqueue_exc:
            reason = (
                f"Enqueue error for DLQ job '{original_job_id}' "
                f"(workflow: '{workflow_name}'): {enqueue_exc}"
            )
            logger.error("[REPLAY] %s", reason)
            # The entry was already RPOP-ed; push it back to avoid silent drop.
            try:
                redis_conn.rpush(dlq_key, raw)
                reason += " [entry restored to DLQ]"
            except Exception as restore_exc:
                reason += f" [DLQ restore also failed: {restore_exc}]"
            skipped += 1
            skipped_details.append(reason)

    total_processed = requeued + skipped
    logger.info(
        "[REPLAY] Complete — %d requeued, %d skipped, %d total drained from '%s'.",
        requeued,
        skipped,
        total_processed,
        dlq_key,
    )
    return {
        "requeued": requeued,
        "skipped": skipped,
        "total_processed": total_processed,
        "skipped_details": skipped_details,
    }
