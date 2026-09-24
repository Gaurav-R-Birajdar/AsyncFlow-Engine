"""
AsyncFlow Engine — Governance Interceptor (Phase 2: MCP Governance Layer).

Responsibility:
  Sit between LLM output and downstream tool execution.  Any JSON payload
  produced by an EXTRACT_JSON or CUSTOM_PROMPT step is evaluated against a
  set of Pydantic-backed PII schemas.  Sensitive fields are **aggressively
  redacted** (replaced with ``[REDACTED_BY_POLICY]``) before the value is
  allowed to propagate to the next chain step or be returned to the caller.

Architecture:
  queue_tasks.py  →  _run_llm_step()  →  [raw JSON str]
                                              ↓
                                  GovernanceInterceptor.sanitize_payload()
                                              ↓
                                  AuditLogger.log_event()  →  data/audit.jsonl
                                              ↓
                                  [redacted JSON str]  →  next step / return

Fault model:
  - Un-parseable JSON input       → raises GovernanceError
  - Pydantic schema load failure  → raises GovernanceError
  - Both are caught by queue_tasks' except-all, which re-raises for RQ retry
    and, on exhaustion, routes the job to the Dead-Letter Queue via route_to_dlq.

Design invariants:
  - Zero mutation of payloads that contain no PII fields.
  - ``sanitize_payload`` is a pure function w.r.t. the audit log — it does NOT
    write to ``data/audit.jsonl``; that responsibility belongs to AuditLogger.
  - PII field detection is case-insensitive and key-path aware (nested dicts
    are recursively scanned).
  - The ``[REDACTED_BY_POLICY]`` sentinel is a plain string — JSON-serialisable
    and self-documenting to any downstream consumer.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sentinel value
# ---------------------------------------------------------------------------

_REDACTED_SENTINEL: str = "[REDACTED_BY_POLICY]"

# ---------------------------------------------------------------------------
# Canonical PII field registry  (case-insensitive key matching)
# ---------------------------------------------------------------------------

#: All keys that are unconditionally redacted, regardless of nesting depth.
#: Extend this set to add new policy-controlled fields without touching
#: ``GovernanceInterceptor`` logic.
PII_FIELD_NAMES: frozenset[str] = frozenset(
    {
        "tax_id",
        "ssn",
        "social_security_number",
        "employee_name",
        "full_name",
        "email",
        "email_address",
        "phone",
        "phone_number",
        "mobile",
        "date_of_birth",
        "dob",
        "account_number",
        "bank_account",
        "credit_card",
        "passport_number",
        "national_id",
        "driver_license",
        "ip_address",
        "salary",
        "compensation",
    }
)


# ---------------------------------------------------------------------------
# Pydantic PII Schema — EnterpriseToolPayload
# ---------------------------------------------------------------------------


class EnterpriseToolPayload(BaseModel):
    """
    Pydantic schema representing a structured payload that *may* carry PII.

    Why Optional everywhere?
      The interceptor must detect **partial** matches — a payload that only
      contains ``ssn`` should still be flagged even if it lacks ``tax_id``.
      Setting all fields to ``str | None = None`` makes the model accept any
      subset of PII fields without raising a validation error for absent ones.

    Why not use ``model_config = ConfigDict(extra='allow')``?
      We deliberately allow extra fields (``extra='allow'``) so that the model
      can be hydrated from an *arbitrary* LLM output dict.  Only the declared
      fields trigger PII detection; undeclared fields pass through untouched
      unless a recursive key-scan catches them.
    """

    # Declared PII fields — add new ones here to extend the schema
    tax_id: str | None = None
    ssn: str | None = None
    social_security_number: str | None = None
    employee_name: str | None = None
    full_name: str | None = None
    email: str | None = None
    email_address: str | None = None
    phone: str | None = None
    phone_number: str | None = None
    mobile: str | None = None
    date_of_birth: str | None = None
    dob: str | None = None
    account_number: str | None = None
    bank_account: str | None = None
    credit_card: str | None = None
    passport_number: str | None = None
    national_id: str | None = None
    driver_license: str | None = None
    ip_address: str | None = None
    salary: str | None = None
    compensation: str | None = None

    model_config = {"extra": "allow"}


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class GovernanceError(RuntimeError):
    """
    Raised by ``GovernanceInterceptor`` when the interceptor cannot safely
    process a payload.

    Common causes:
      - Input string is not valid JSON (un-parseable LLM output).
      - Pydantic model instantiation fails (schema misconfiguration).

    Why RuntimeError?
      The existing ``except (LLMTimeoutError, LLMMalformedResponseError, Exception)``
      catch-all in ``queue_tasks.process_workflow`` catches RuntimeError subtypes
      and re-raises them, which activates RQ's exponential backoff + DLQ routing
      without any special-casing.
    """


# ---------------------------------------------------------------------------
# GovernanceInterceptor
# ---------------------------------------------------------------------------


class GovernanceInterceptor:
    """
    Middleware interceptor that evaluates LLM JSON payloads against the
    enterprise PII policy and redacts any sensitive fields in-place.

    Instantiated once per workflow run in ``process_workflow`` to avoid
    repeated object construction overhead.

    Usage::

        interceptor = GovernanceInterceptor()
        sanitized, redacted_keys = interceptor.sanitize_payload(raw_dict, "step_id")
        if redacted_keys:
            logger.warning("[GOVERNANCE_INTERCEPT] ...")
    """

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def sanitize_payload(
        self,
        payload: dict[str, Any],
        tool_name: str,
    ) -> tuple[dict[str, Any], list[str]]:
        """
        Evaluate ``payload`` against the PII policy and redact sensitive fields.

        Algorithm:
          1. Attempt to hydrate ``EnterpriseToolPayload`` from ``payload`` to
             confirm the dict is structurally valid.  Raises ``GovernanceError``
             on Pydantic ``ValidationError`` (schema misconfiguration — should
             not happen in production; indicates a code defect).
          2. Recursively walk every key in the dict (handles nested objects).
             For each key whose lowercase form appears in ``PII_FIELD_NAMES``,
             replace the value with ``[REDACTED_BY_POLICY]`` and record the
             dot-notation path in ``redacted_keys``.
          3. Return the mutated dict and the list of redacted key paths.

        Args:
            payload:   A parsed JSON dict (output of ``json.loads``).
            tool_name: The step_id or tool name — used for audit trail context.

        Returns:
            ``(sanitized_payload, redacted_keys)`` where ``sanitized_payload``
            is the (possibly mutated) dict and ``redacted_keys`` is a list of
            dot-path strings for every field that was redacted.

        Raises:
            GovernanceError: Pydantic schema validation failed.
        """
        try:
            # Validate structural compatibility — does not mutate ``payload``
            EnterpriseToolPayload(**payload)
        except ValidationError as exc:
            # Schema validation failure is a code defect, not a PII event.
            # Raise GovernanceError to trigger the DLQ fault path.
            raise GovernanceError(
                f"GovernanceInterceptor: Pydantic validation failed for tool '{tool_name}': {exc}"
            ) from exc
        except TypeError as exc:
            # payload is not a flat dict (e.g. nested model instantiation issue)
            raise GovernanceError(
                f"GovernanceInterceptor: TypeError during schema validation for tool '{tool_name}': {exc}"
            ) from exc

        # Deep-scan and redact PII fields; returns mutated copy + path list
        sanitized, redacted_keys = self._redact_recursive(payload, parent_path="")

        logger.debug(
            "GovernanceInterceptor: tool='%s' scanned — %d PII field(s) detected.",
            tool_name,
            len(redacted_keys),
        )

        return sanitized, redacted_keys

    def parse_and_sanitize(
        self,
        raw_json: str,
        tool_name: str,
    ) -> tuple[dict[str, Any], list[str]]:
        """
        Parse a raw JSON string and immediately sanitize it.

        Convenience wrapper used by ``queue_tasks`` so the caller does not
        need to handle ``json.loads`` itself.

        Args:
            raw_json:  The JSON string emitted by ``_run_llm_step()``.
            tool_name: Step identifier for audit context.

        Returns:
            ``(sanitized_dict, redacted_keys)``

        Raises:
            GovernanceError: JSON decode failed or Pydantic validation failed.
        """
        try:
            payload: dict[str, Any] = json.loads(raw_json)
        except json.JSONDecodeError as exc:
            raise GovernanceError(
                f"GovernanceInterceptor: Cannot parse LLM output as JSON for tool "
                f"'{tool_name}'. Raw (first 300 chars): {raw_json[:300]!r}. "
                f"Detail: {exc}"
            ) from exc

        if not isinstance(payload, dict):
            raise GovernanceError(
                f"GovernanceInterceptor: LLM output for tool '{tool_name}' is valid JSON "
                f"but not a dict (got {type(payload).__name__}). Cannot apply PII policy."
            )

        return self.sanitize_payload(payload, tool_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _redact_recursive(
        self,
        node: Any,
        parent_path: str,
    ) -> tuple[Any, list[str]]:
        """
        Recursively walk ``node`` and redact any dict key in ``PII_FIELD_NAMES``.

        Why recursion instead of a flat scan?
          LLM outputs frequently nest objects (e.g. ``{"employee": {"ssn": …}}``).
          A flat scan would miss PII buried at depth > 1.

        Args:
            node:        Current value being inspected (any JSON type).
            parent_path: Dot-notation path accumulated from root (e.g. ``"employee"``).

        Returns:
            ``(mutated_node, redacted_key_paths)``
        """
        redacted: list[str] = []

        if isinstance(node, dict):
            mutated: dict[str, Any] = {}
            for key, value in node.items():
                current_path = f"{parent_path}.{key}" if parent_path else key

                if key.lower() in PII_FIELD_NAMES and value is not None:
                    # Redact — replace the value, record the path
                    mutated[key] = _REDACTED_SENTINEL
                    redacted.append(current_path)
                    logger.debug(
                        "GovernanceInterceptor: Redacted field '%s'.", current_path
                    )
                else:
                    # Recurse into nested objects / lists
                    child_node, child_redacted = self._redact_recursive(value, current_path)
                    mutated[key] = child_node
                    redacted.extend(child_redacted)
            return mutated, redacted

        elif isinstance(node, list):
            mutated_list: list[Any] = []
            for i, item in enumerate(node):
                item_path = f"{parent_path}[{i}]"
                child_node, child_redacted = self._redact_recursive(item, item_path)
                mutated_list.append(child_node)
                redacted.extend(child_redacted)
            return mutated_list, redacted

        # Scalar (str, int, float, bool, None) — no redaction at this level
        return node, redacted
