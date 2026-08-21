"""
AsyncFlow Engine — FastAPI Application Instance & Routing.

Responsibilities:
  - Initialise the FastAPI app with metadata.
  - Register all API routers.
  - Manage application lifespan (startup / shutdown hooks).
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as workflow_router
from app.core.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and graceful shutdown."""
    # TODO: Initialise Redis connection pool on startup
    print(f"[AsyncFlow] Starting up — Redis @ {settings.REDIS_URL}")
    yield
    # TODO: Flush any pending jobs or close connections on shutdown
    print("[AsyncFlow] Shutting down — goodbye.")


app = FastAPI(
    title="AsyncFlow Engine",
    description=(
        "A high-throughput async workflow orchestration engine. "
        "Submit multi-step LLM pipelines and poll their status in real time."
    ),
    version="0.0.1",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Tighten in production
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
    """Lightweight liveness probe — returns 200 if the service is up."""
    return {"status": "ok", "version": app.version}
