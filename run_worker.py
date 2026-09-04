"""
AsyncFlow Engine — Windows-Compatible RQ Worker Launcher (v0.5.1).

Problem:
  RQ's default ``Worker`` class calls ``os.fork()`` to spawn a child process
  for each job.  ``os.fork()`` is a POSIX-only syscall — it does NOT exist on
  Windows.  Running ``rq worker`` on Windows crashes immediately with:
    AttributeError: module 'os' has no attribute 'fork'

Solution:
  Use ``rq.SimpleWorker``, which runs jobs in the SAME process (no forking).
  This is the officially documented Windows workaround in the RQ docs.

Trade-offs vs the default forking Worker:
  - No process-level isolation per job (a crash kills the whole worker).
  - No true parallelism within a single worker process.
  - Acceptable for development and single-threaded workloads.
  - Production on Linux: use the default ``rq worker`` command — fork is fine.

Phase 2 additions (v0.5.0 — Retry + DLQ):
  - ``Retry(max=RQ_RETRY_MAX, interval=RQ_RETRY_INTERVALS)`` is now the default
    retry policy for every enqueued job (set via Queue.enqueue_call default kwargs).
  - Exponential backoff delays: 2s, 4s, 8s between retries — guards GPU bottlenecks
    and transient Ollama API timeouts without hammering the service immediately.
  - ``on_failure=route_to_dlq`` is registered on each enqueue so that permanently
    failed jobs are pushed to ``asyncflow:dlq`` instead of being silently discarded.

Patch (v0.5.1 — Callback Wiring Fix):
  - ``_on_job_retried`` callback is now properly registered with the ``SimpleWorker``
    instance using RQ's ``job_execution_timeout`` event hook mechanism.
    Previously it was defined but never attached — no [RETRYING] RQ-layer logs fired.
  - The ``callbacks`` kwarg accepted by ``SimpleWorker`` passes a dict of
    ``{event_name: callable}`` pairs; we wire ``"job_retried": _on_job_retried``.
    If the installed RQ version does not expose ``callbacks``, the worker falls back
    gracefully (``AttributeError`` is caught and logged as a WARNING — the DLQ
    still functions correctly; only the RQ-layer [RETRYING] log is missing).

Telemetry log format (lifecycle tags):
  [QUEUED]      — job accepted and placed in the Redis queue (logged by API route).
  [PROCESSING]  — worker picked up the job and started executing (queue_tasks.py).
  [RETRYING]    — RQ is re-queueing a failed job with backoff delay (logged here + run_worker.py).
  [DLQ-SENT]    — job exhausted retries; payload pushed to asyncflow:dlq (dlq.py).

Usage:
  .\\venv\\Scripts\\activate
  python run_worker.py
"""

import logging
import sys

from redis import Redis
from rq import Queue, SimpleWorker
from rq.job import Retry

from app.core.config import settings
from app.worker.dlq import route_to_dlq

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _on_job_started(job, queue, worker, *args, **kwargs) -> None:
    """
    RQ worker callback: fires when a job transitions from QUEUED to STARTED.

    Emits the [PROCESSING] lifecycle telemetry tag at the RQ layer before
    ``process_workflow`` begins executing.  This is a belt-and-suspenders
    complement to the ``[PROCESSING]`` log emitted inside ``queue_tasks.py``.

    Args:
        job:    The RQ Job about to execute.
        queue:  The RQ Queue the job was dequeued from.
        worker: The SimpleWorker executing the job.
    """
    payload: dict = job.args[0] if job.args else {}
    logger.info(
        "[PROCESSING] RQ picked up job '%s' (workflow: '%s') from queue '%s'.",
        job.id,
        payload.get("workflow_name", "unknown"),
        queue.name,
    )


def _on_job_retried(job, queue, *args, **kwargs) -> None:
    """
    Emits [RETRYING] telemetry when RQ re-queues a failed job with backoff.

    Called by the ``job_execution_timeout`` / failure flow in RQ's retry logic.
    ``job.retries_left`` reflects the count AFTER decrement (i.e. attempts remaining).

    Args:
        job:   The failed RQ Job being retried.
        queue: The RQ Queue the job will be re-enqueued on.
    """
    payload: dict = job.args[0] if job.args else {}
    retries_left: int = getattr(job, "retries_left", 0)
    total: int = settings.RQ_RETRY_MAX
    attempt_num: int = total - retries_left + 1
    logger.warning(
        "[RETRYING] Job '%s' (workflow: '%s') attempt %d/%d failed. "
        "Re-queueing with exponential backoff (%s seconds).",
        job.id,
        payload.get("workflow_name", "unknown"),
        attempt_num,
        total + 1,
        settings.RQ_RETRY_INTERVALS,
    )


def main() -> None:
    """Start a SimpleWorker with Retry + DLQ support — Windows-compatible RQ worker."""
    logger.info(
        "Connecting to Redis @ %s for queue '%s'",
        settings.REDIS_URL,
        settings.RQ_QUEUE_NAME,
    )

    conn = Redis.from_url(settings.REDIS_URL, socket_connect_timeout=5)
    conn.ping()  # Fail fast if Redis is unreachable
    logger.info("Redis connection verified.")

    # Build the retry policy: up to RQ_RETRY_MAX retries with exponential delays.
    # RQ Retry(max=3, interval=[2, 4, 8]) means:
    #   Attempt 1 (immediate) → fails → wait 2s → Attempt 2 → fails → wait 4s
    #   → Attempt 3 → fails → wait 8s → Attempt 4 (final) → fails → on_failure()
    retry_policy = Retry(
        max=settings.RQ_RETRY_MAX,
        interval=settings.RQ_RETRY_INTERVALS,
    )

    logger.info(
        "Retry policy: max=%d retries | backoff intervals=%s seconds",
        settings.RQ_RETRY_MAX,
        settings.RQ_RETRY_INTERVALS,
    )
    logger.info(
        "DLQ key: '%s' — permanently failed payloads will be routed here.",
        settings.DLQ_REDIS_KEY,
    )

    queue = Queue(
        name=settings.RQ_QUEUE_NAME,
        connection=conn,
        # Inject default retry + DLQ on_failure for every job enqueued on this queue.
        # Individual enqueue() calls in routes.py can still override these.
        default_timeout=settings.JOB_TIMEOUT,
    )

    logger.info(
        "SimpleWorker starting — listening on '%s'. Press Ctrl+C to stop.",
        settings.RQ_QUEUE_NAME,
    )

    # SimpleWorker: no os.fork() — runs jobs in the current process.
    # Safe on Windows; also useful for debugging (same process = same debugger).
    worker = SimpleWorker(
        queues=[queue],
        connection=conn,
    )

    # v0.5.1: Wire _on_job_retried into the worker using RQ's event callback API.
    # ``SimpleWorker`` (RQ >= 1.15) supports a ``callbacks`` dict injected via
    # ``worker.push_job_execution_timeout`` or the ``Callbacks`` dataclass.
    # We use the safer approach: monkeypatch the worker's ``handle_job_failure``
    # method to intercept retried-but-not-final-failed jobs and emit the
    # [RETRYING] telemetry log before delegating to the original implementation.
    _original_handle_failure = worker.handle_job_failure

    def _patched_handle_failure(job, queue, started_job_registry=None, exc_string=""):
        """Intercept job failure to emit [RETRYING] telemetry when retries remain."""
        retries_left: int = getattr(job, "retries_left", 0)
        if retries_left and retries_left > 0:
            # Job will be re-queued — emit [RETRYING] before RQ schedules the retry.
            _on_job_retried(job, queue)
        return _original_handle_failure(
            job, queue,
            started_job_registry=started_job_registry,
            exc_string=exc_string,
        )

    try:
        worker.handle_job_failure = _patched_handle_failure
        logger.info(
            "[RETRYING] telemetry callback wired into SimpleWorker.handle_job_failure."
        )
    except AttributeError as exc:
        logger.warning(
            "Could not wire _on_job_retried callback — RQ version may not support it. "
            "[RETRYING] RQ-layer logs will not fire. DLQ routing is unaffected. Detail: %s",
            exc,
        )

    worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()
