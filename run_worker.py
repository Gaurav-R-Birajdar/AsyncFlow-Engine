"""
AsyncFlow Engine — Windows-Compatible RQ Worker Launcher.

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

Usage:
  .\venv\Scripts\activate
  python run_worker.py
"""

import logging
import sys

from redis import Redis
from rq import Queue, SimpleWorker

from app.core.config import settings

logging.basicConfig(
    level=settings.LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start a SimpleWorker — the Windows-compatible RQ worker."""
    logger.info(
        "Connecting to Redis @ %s for queue '%s'",
        settings.REDIS_URL,
        settings.RQ_QUEUE_NAME,
    )

    conn = Redis.from_url(settings.REDIS_URL, socket_connect_timeout=5)
    conn.ping()  # Fail fast if Redis is unreachable
    logger.info("Redis connection verified.")

    queue = Queue(name=settings.RQ_QUEUE_NAME, connection=conn)

    logger.info(
        "SimpleWorker starting — listening on '%s'. Press Ctrl+C to stop.",
        settings.RQ_QUEUE_NAME,
    )

    # SimpleWorker: no os.fork() — runs jobs in the current process.
    # Safe on Windows; also useful for debugging (same process = same debugger).
    worker = SimpleWorker(queues=[queue], connection=conn)
    worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()
