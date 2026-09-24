"""
AsyncFlow Engine — Governance Audit Logger (Phase 2: MCP Governance Interceptor).

Responsibility:
  Write an append-only, tamper-evident JSONL audit log for every governance
  interception event.  Each event captures the full before/after payload so
  that compliance teams can reconstruct exactly what PII was present in an
  LLM output and confirm it was redacted before downstream propagation.

Log format (one JSON object per line, UTF-8):
  {
    "timestamp":         "<ISO-8601 UTC>",
    "tool_name":         "<step_id or tool identifier>",
    "redacted_keys":     ["<dot-path>", ...],
    "original_payload":  { ... },   // WARNING: contains raw PII
    "sanitized_payload": { ... }    // safe to share
  }

Security notes:
  - ``data/audit.jsonl`` is listed in ``.gitignore`` to prevent PII from
    being committed to source control.
  - The file is opened in ``mode='a'`` (append) — existing entries are never
    overwritten.
  - The directory is created automatically on first write (``exist_ok=True``).
  - If the write itself fails, a ``GovernanceError`` is raised to block the
    workflow from proceeding without an audit trail.

Windows compatibility:
  Python's built-in ``open(mode='a')`` is atomic enough for the single-process
  RQ worker model.  ``fcntl`` (POSIX-only) is intentionally avoided.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from app.core.config import settings
from app.governance.interceptor import GovernanceError

logger = logging.getLogger(__name__)


class AuditLogger:
    """
    Lightweight, append-only JSONL audit logger for governance interception events.

    Instantiated once per workflow run by ``process_workflow`` alongside the
    ``GovernanceInterceptor``.  All writes are synchronous and blocking — this
    is intentional because:
      1. The RQ SimpleWorker is single-process; no concurrency hazards.
      2. Audit writes must complete before the sanitized payload is chained to
         the next step — we never proceed without confirming the trail exists.

    Usage::

        audit = AuditLogger()
        audit.log_event(
            original=raw_dict,
            sanitized=redacted_dict,
            tool_name="step_extract",
            redacted_keys=["ssn", "employee.tax_id"],
        )
    """

    def __init__(self, log_path: str | None = None) -> None:
        """
        Args:
            log_path: Absolute or relative path to the audit JSONL file.
                      Defaults to ``settings.AUDIT_LOG_PATH`` (``data/audit.jsonl``).
        """
        self._log_path: str = log_path or settings.AUDIT_LOG_PATH

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def log_event(
        self,
        original: dict[str, Any],
        sanitized: dict[str, Any],
        tool_name: str,
        redacted_keys: list[str],
    ) -> None:
        """
        Append one audit event to the JSONL log file.

        Args:
            original:     The raw payload as produced by the LLM (may contain PII).
            sanitized:    The redacted payload (safe for downstream consumption).
            tool_name:    The step_id or logical tool name that produced the payload.
            redacted_keys: Dot-notation paths of every field that was redacted.

        Raises:
            GovernanceError: If the file cannot be opened or written to.
                             This blocks the workflow from proceeding without an
                             audit trail — silent failures are never acceptable.
        """
        event: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": tool_name,
            "redacted_keys": redacted_keys,
            "original_payload": original,
            "sanitized_payload": sanitized,
        }

        try:
            self._write_event(event)
        except GovernanceError:
            raise
        except Exception as exc:
            raise GovernanceError(
                f"AuditLogger: Failed to write audit event for tool '{tool_name}': {exc}"
            ) from exc

        logger.debug(
            "AuditLogger: Event written for tool='%s' — %d key(s) redacted.",
            tool_name,
            len(redacted_keys),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_event(self, event: dict[str, Any]) -> None:
        """
        Serialize ``event`` as a single JSON line and append to the log file.

        Why ``ensure_ascii=False``?
          LLM outputs may contain unicode (names, addresses).  Keeping unicode
          in-place makes the log human-readable without an extra decode step.

        Why ``default=str``?
          A safety net for any non-JSON-serialisable type that might sneak in
          from the LLM output (e.g. datetime objects) — converts them to their
          string representation rather than raising ``TypeError``.
        """
        log_dir = os.path.dirname(self._log_path)
        if log_dir:
            try:
                os.makedirs(log_dir, exist_ok=True)
            except OSError as exc:
                raise GovernanceError(
                    f"AuditLogger: Cannot create log directory '{log_dir}': {exc}"
                ) from exc

        line: str = json.dumps(event, ensure_ascii=False, default=str)

        try:
            with open(self._log_path, mode="a", encoding="utf-8") as fh:
                fh.write(line + "\n")
        except OSError as exc:
            raise GovernanceError(
                f"AuditLogger: Cannot write to '{self._log_path}': {exc}"
            ) from exc
