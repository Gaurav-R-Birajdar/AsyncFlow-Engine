"""
AsyncFlow Engine — FastAPI Application Instance & Routing (v1.0.0).

Responsibilities:
  - Initialise the FastAPI app with metadata.
  - Register all API routers.
  - Manage application lifespan:
      - Startup: Open Redis connection, initialise RQ Queue, verify connectivity.
      - Shutdown: Close Redis connection gracefully.

Why two Redis objects?
  RQ requires a synchronous ``redis.Redis`` client internally.
  FastAPI routes access it via ``request.app.state`` — no global singletons,
  no import-time side effects, clean testability.

v1.0.0 additions (Phase 2 — MCP Governance Interceptor):
  - ``GovernanceInterceptor`` sits between LLM output and step chaining for
    EXTRACT_JSON and CUSTOM_PROMPT steps; recursively redacts PII field values
    with ``[REDACTED_BY_POLICY]``.
  - ``AuditLogger`` writes an append-only JSONL record to ``data/audit.jsonl``
    for every governance interception event.
  - ``GOVERNANCE_ENABLED`` kill-switch in config allows bypassing the interceptor
    in local development without code changes.
  - Version bumped to ``1.0.0``.

v0.6.1 patch (retained — critical fix):
  - ``process_workflow`` now re-raises the causal exception after fatal step
    failures so RQ exponential backoff and DLQ routing activate correctly.

v0.6.0 additions (Phase 3 — DLQ Replay):
  - ``POST /dlq/replay``     — admin endpoint that drains the DLQ and re-enqueues
                               each payload as a fresh RQ job with a reset retry
                               counter (idempotent: atomic RPOP prevents double-requeue).
  - ``DlqReplayResponse``    — new Pydantic schema surfacing requeued/skipped counts.

v0.5.0–v0.5.1 additions (retained):
  - ``GET /workflow/dlq`` / ``GET /dlq`` — DLQ inspection endpoints.
  - RQ Retry with exponential backoff (2s/4s/8s) wired at enqueue time.
  - ``route_to_dlq`` registered as on_failure callback for all jobs.
"""

import json
import logging
import sys
from contextlib import asynccontextmanager
from functools import partial

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from redis import ConnectionError as RedisConnectionError
from redis import Redis
from rq import Queue

from app.api.routes import router as workflow_router
from app.core.config import settings
from app.core.schemas import DlqReplayResponse
from app.worker.dlq import replay_dlq

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handle application startup and graceful shutdown.

    Startup sequence:
      1. Create a synchronous Redis connection (required by RQ).
      2. Ping Redis to verify connectivity — fail fast if unreachable.
      3. Initialise the RQ Queue and store both on ``app.state``.

    Shutdown sequence:
      1. Close the Redis connection to release the socket descriptor.
    """
    # --- Startup -----------------------------------------------------------
    logger.info("AsyncFlow Engine starting up — connecting to Redis @ %s", settings.REDIS_URL)

    try:
        redis_conn = Redis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=5,   # Fail fast if Redis is unreachable
            decode_responses=False,     # RQ requires bytes, not str
        )
        redis_conn.ping()  # Raises ConnectionError if Redis is down
        logger.info("Redis connection established.")
    except RedisConnectionError as exc:
        logger.critical(
            "Cannot connect to Redis at '%s': %s. "
            "Start Redis (docker-compose up -d) and retry.",
            settings.REDIS_URL,
            exc,
        )
        # Re-raise to prevent FastAPI from serving requests with no queue
        raise

    rq_queue = Queue(
        name=settings.RQ_QUEUE_NAME,
        connection=redis_conn,
        default_timeout=settings.JOB_TIMEOUT,
    )

    # Inject into app state — routes access via request.app.state
    app.state.redis_conn = redis_conn
    app.state.queue = rq_queue

    logger.info(
        "RQ queue '%s' ready. Service is live.",
        settings.RQ_QUEUE_NAME,
    )

    yield  # ← Application is running

    # --- Shutdown ----------------------------------------------------------
    logger.info("AsyncFlow Engine shutting down — closing Redis connection.")
    redis_conn.close()
    logger.info("Redis connection closed. Goodbye.")


# ---------------------------------------------------------------------------
# Application Instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="AsyncFlow Engine",
    description=(
        "A high-throughput async workflow orchestration engine. "
        "Submit multi-step LLM pipelines and poll their status in real time. "
        "v1.0.0 (Phase 2): MCP Governance Interceptor — PII redaction and append-only "
        "audit logging for structured LLM outputs before downstream propagation. "
        "v0.6.1 fix: LLM connection failures now correctly re-raise exceptions "
        "so RQ exponential backoff and DLQ routing activate as designed. "
        "v0.6.0 (Phase 3): DLQ Replay Mechanism — POST /dlq/replay re-enqueues "
        "failed jobs as fresh RQ tasks with reset retry counters."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten to specific origins in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(workflow_router, prefix="/workflow", tags=["Workflow"])


@app.get("/health", tags=["Meta"])
async def health_check() -> dict:
    """
    Lightweight liveness probe.

    Returns 200 if the service is up.  Clients can also check queue depth
    via ``/workflow/submit`` submission counts or dedicated metrics.
    """
    return {"status": "ok", "version": app.version}


@app.get(
    "/dlq",
    summary="Inspect permanently failed workflow payloads (top-level)",
    tags=["Dead-Letter Queue"],
    response_model=list[dict],
)
async def get_dlq_top_level(
    request: Request,
    limit: int = Query(
        default=settings.DLQ_MAX_ENTRIES,
        ge=1,
        le=1000,
        description="Maximum number of DLQ entries to return (newest-first). Default: DLQ_MAX_ENTRIES.",
    ),
) -> list[dict]:
    """
    Fetch DLQ entries from the ``asyncflow:dlq`` Redis list.

    This is the **top-level** ``GET /dlq`` route — identical in behaviour to
    ``GET /workflow/dlq`` but mounted directly on the root app (not under the
    ``/workflow`` prefix) to avoid collision with the ``/{task_id}/status``
    wildcard route and to provide a cleaner public API surface.

    Query Parameters:
        limit: Cap on entries returned (1–1000). Defaults to ``DLQ_MAX_ENTRIES``
               from settings (default 500).  Use this to avoid transferring
               a large list over the wire during an outage storm.

    Returns:
        A list of DLQ entry dicts, ordered newest-first (LPUSH head = index 0).
        Returns an empty list if the DLQ is empty.

    Raises:
        HTTPException(503): If Redis is unreachable during the fetch.
    """
    redis_conn = request.app.state.redis_conn
    dlq_key: str = settings.DLQ_REDIS_KEY

    try:
        # LRANGE 0 (limit-1) returns at most `limit` entries without blocking.
        raw_entries: list[bytes] = redis_conn.lrange(dlq_key, 0, limit - 1)
    except Exception as exc:
        logger.error(
            "Redis error while fetching DLQ key '%s' (top-level route): %s",
            dlq_key,
            exc,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not read from the Dead-Letter Queue. Ensure Redis is running.",
        )

    parsed: list[dict] = []
    for raw in raw_entries:
        try:
            parsed.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(
                "DLQ entry could not be decoded: %s | raw: %r", exc, raw
            )
            parsed.append({"error": "malformed DLQ entry", "raw": str(raw)})

    logger.info(
        "GET /dlq — returned %d/%d DLQ entr%s from key '%s'.",
        len(parsed),
        limit,
        "y" if len(parsed) == 1 else "ies",
        dlq_key,
    )
    return parsed


@app.post(
    "/dlq/replay",
    response_model=DlqReplayResponse,
    status_code=status.HTTP_200_OK,
    summary="Replay all failed DLQ jobs back into the active queue",
    tags=["Dead-Letter Queue"],
)
async def replay_dlq_endpoint(request: Request) -> DlqReplayResponse:
    """
    Drain the Dead-Letter Queue and re-enqueue every failed payload as a
    fresh RQ job with a fully reset retry counter.

    **Admin use-case**: After recovering from a GPU outage or LLM API timeout,
    call this endpoint to push all accumulated failures back into the primary
    ``asyncflow_default`` queue for reprocessing — no manual intervention needed.

    **Idempotency & safety**:
    - Each entry is atomically ``RPOP``-ed before re-enqueueing, so concurrent
      calls cannot double-requeue the same job.
    - A fresh ``Retry`` policy is attached (``retries_left = RQ_RETRY_MAX``),
      clearing all prior failure metadata.
    - If re-enqueue fails (e.g., transient Redis write error), the entry is
      pushed back to the DLQ tail and counted as ``skipped`` — no silent data loss.
    - Malformed entries (non-JSON, missing ``payload``) are skipped and logged;
      they are **not** returned to the DLQ to prevent poison-pill infinite loops.

    Returns:
        ``DlqReplayResponse`` with:
        - ``requeued``        — jobs successfully re-admitted to the active queue.
        - ``skipped``         — entries that could not be re-enqueued.
        - ``total_processed`` — total DLQ entries drained (requeued + skipped).
        - ``message``         — human-readable summary.
        - ``skipped_details`` — per-skipped-entry diagnostic strings.

    Raises:
        HTTPException(503): If the Redis connection is unavailable.
    """
    redis_conn = request.app.state.redis_conn
    rq_queue = request.app.state.queue

    # replay_dlq is synchronous (blocking Redis calls + RQ enqueue).
    # Offload to a thread pool so we don't block the async event loop.
    import asyncio
    loop = asyncio.get_running_loop()
    try:
        result: dict = await loop.run_in_executor(
            None,
            partial(replay_dlq, redis_conn, rq_queue),
        )
    except Exception as exc:
        logger.error("[REPLAY] Unexpected error during DLQ replay: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"DLQ replay failed: {exc}. Ensure Redis is running.",
        )

    requeued: int = result["requeued"]
    skipped: int = result["skipped"]
    total: int = result["total_processed"]

    if total == 0:
        message = "DLQ is empty — no jobs to replay."
    elif skipped == 0:
        message = f"Replay complete: {requeued} job(s) successfully re-enqueued."
    else:
        message = (
            f"Replay complete: {requeued} job(s) re-enqueued, "
            f"{skipped} skipped (see skipped_details)."
        )

    logger.info("[REPLAY] POST /dlq/replay — %s", message)
    return DlqReplayResponse(
        requeued=requeued,
        skipped=skipped,
        total_processed=total,
        message=message,
        skipped_details=result["skipped_details"],
    )
