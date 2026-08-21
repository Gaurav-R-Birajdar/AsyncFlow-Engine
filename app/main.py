"""
AsyncFlow Engine — FastAPI Application Instance & Routing.

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
"""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from redis import ConnectionError as RedisConnectionError
from redis import Redis
from rq import Queue

from app.api.routes import router as workflow_router
from app.core.config import settings

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
        "Submit multi-step LLM pipelines and poll their status in real time."
    ),
    version="0.1.0",
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
