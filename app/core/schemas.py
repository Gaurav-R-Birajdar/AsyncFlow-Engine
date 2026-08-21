"""
AsyncFlow Engine — Pydantic Schemas (V1).

This file is the single source of truth for every JSON shape that crosses
the API boundary.  All models use strict validation so malformed payloads
are rejected at the edge — before they ever touch the queue or the LLM.

Design goals:
  1. Composable  — Steps are independent units; chains are just ordered lists.
  2. Extensible  — Adding a new task type only requires a new TaskType member
                   and a matching StepConfig variant (discriminated union).
  3. Self-documenting — Field descriptions power the auto-generated OpenAPI UI.

Workflow lifecycle:
  User submits WorkflowSubmitRequest
    → validated here
    → enqueued in RQ
    → each WorkflowStep executed sequentially by the worker
    → final state captured in WorkflowStatusResponse
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class TaskType(str, Enum):
    """Supported atomic task types within a workflow step."""

    SUMMARIZE = "summarize"
    TRANSLATE = "translate"
    EXTRACT_JSON = "extract_json"
    CLASSIFY = "classify"
    SENTIMENT = "sentiment"
    CUSTOM_PROMPT = "custom_prompt"


class WorkflowStatus(str, Enum):
    """Lifecycle states of an enqueued workflow."""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepStatus(str, Enum):
    """Per-step execution state (mirrors WorkflowStatus but scoped to a step)."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


# ---------------------------------------------------------------------------
# Step Configurations  (discriminated union — extensible by design)
# ---------------------------------------------------------------------------


class SummarizeConfig(BaseModel):
    """Configuration for a text-summarisation step."""

    task_type: Literal[TaskType.SUMMARIZE] = TaskType.SUMMARIZE
    max_words: Annotated[int, Field(ge=10, le=2000, description="Target word count for the summary")] = 150
    style: Annotated[
        str,
        Field(description="Summary style hint, e.g. 'bullet points', 'narrative', 'executive'"),
    ] = "narrative"


class TranslateConfig(BaseModel):
    """Configuration for a translation step."""

    task_type: Literal[TaskType.TRANSLATE] = TaskType.TRANSLATE
    target_language: Annotated[
        str,
        Field(min_length=2, max_length=50, description="BCP-47 language tag, e.g. 'fr', 'de', 'ja'"),
    ]
    preserve_formatting: bool = True


class ExtractJsonConfig(BaseModel):
    """
    Configuration for a structured JSON-extraction step.

    Why ``output_schema``?
      Providing a JSON Schema lets the worker instruct the LLM to produce
      output that strictly conforms to a caller-defined structure.
    """

    task_type: Literal[TaskType.EXTRACT_JSON] = TaskType.EXTRACT_JSON
    output_schema: Annotated[
        dict[str, Any],
        Field(description="JSON Schema dict describing the expected output structure"),
    ]
    strict: bool = Field(
        default=True,
        description="If True, worker will retry / fail rather than return malformed JSON",
    )


class ClassifyConfig(BaseModel):
    """Configuration for a text-classification step."""

    task_type: Literal[TaskType.CLASSIFY] = TaskType.CLASSIFY
    labels: Annotated[
        list[str],
        Field(min_length=2, description="Exhaustive list of candidate class labels"),
    ]
    multi_label: bool = Field(default=False, description="Allow more than one label per input")


class SentimentConfig(BaseModel):
    """Configuration for a sentiment-analysis step."""

    task_type: Literal[TaskType.SENTIMENT] = TaskType.SENTIMENT
    granularity: Literal["document", "sentence"] = "document"


class CustomPromptConfig(BaseModel):
    """
    Escape-hatch step for arbitrary prompt templates.

    Use ``{input}`` as the placeholder for the previous step's output.
    """

    task_type: Literal[TaskType.CUSTOM_PROMPT] = TaskType.CUSTOM_PROMPT
    prompt_template: Annotated[
        str,
        Field(
            min_length=10,
            description="Jinja2-compatible prompt template.  Use {{ input }} for chained input.",
        ),
    ]
    temperature: Annotated[float, Field(ge=0.0, le=2.0)] = 0.7
    max_tokens: Annotated[int, Field(ge=1, le=8192)] = 512


# Discriminated union — Pydantic resolves the correct model via ``task_type``
StepConfig = (
    SummarizeConfig
    | TranslateConfig
    | ExtractJsonConfig
    | ClassifyConfig
    | SentimentConfig
    | CustomPromptConfig
)


# ---------------------------------------------------------------------------
# Core Workflow Models
# ---------------------------------------------------------------------------


class WorkflowStep(BaseModel):
    """
    A single, atomic unit of work inside a workflow chain.

    Steps are executed **sequentially** by the worker: the ``output`` of
    step N becomes the ``input_text`` of step N+1 (unless overridden).
    """

    step_id: Annotated[
        str,
        Field(
            min_length=1,
            max_length=64,
            pattern=r"^[a-zA-Z0-9_\-]+$",
            description="Unique slug identifying this step within the workflow",
        ),
    ]
    config: Annotated[
        StepConfig,
        Field(discriminator="task_type", description="Task-type-specific configuration"),
    ]
    input_override: Annotated[
        str | None,
        Field(
            default=None,
            description=(
                "If set, this text is used as the step's input instead of the "
                "previous step's output.  Useful for injecting external context."
            ),
        ),
    ] = None
    retry_on_failure: Annotated[bool, Field(description="Retry this step once if it fails")] = False


class WorkflowSubmitRequest(BaseModel):
    """
    Top-level request payload for POST /workflow/submit.

    Example JSON:
    {
      "workflow_name": "summarise-translate-extract",
      "input_text": "The quarterly earnings report shows …",
      "steps": [
        {"step_id": "step_1", "config": {"task_type": "summarize", "max_words": 100}},
        {"step_id": "step_2", "config": {"task_type": "translate", "target_language": "fr"}},
        {"step_id": "step_3", "config": {"task_type": "extract_json",
           "output_schema": {"type": "object", "properties": {"summary_fr": {"type": "string"}}}}}
      ]
    }
    """

    workflow_name: Annotated[
        str,
        Field(
            min_length=1,
            max_length=128,
            pattern=r"^[a-zA-Z0-9_\- ]+$",
            description="Human-readable label for this workflow run",
        ),
    ]
    input_text: Annotated[
        str,
        Field(
            min_length=1,
            max_length=32_000,
            description="The seed text passed to the first step in the chain",
        ),
    ]
    steps: Annotated[
        list[WorkflowStep],
        Field(min_length=1, max_length=20, description="Ordered list of steps to execute"),
    ]
    metadata: dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary caller-supplied key-value pairs (tracing, tagging, etc.)",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator("steps")
    @classmethod
    def step_ids_must_be_unique(cls, steps: list[WorkflowStep]) -> list[WorkflowStep]:
        """Reject duplicate step_ids — they would break result indexing."""
        ids = [s.step_id for s in steps]
        duplicates = {sid for sid in ids if ids.count(sid) > 1}
        if duplicates:
            raise ValueError(f"Duplicate step_id(s) detected: {sorted(duplicates)}")
        return steps

    @model_validator(mode="after")
    def validate_translate_not_first(self) -> "WorkflowSubmitRequest":
        """
        Soft guardrail: warn if a translate step is the very first step.
        Translating raw, potentially noisy input is usually unintentional.
        This can be relaxed in the future via a config flag.
        """
        if self.steps and self.steps[0].config.task_type == TaskType.TRANSLATE:
            raise ValueError(
                "A 'translate' step cannot be the first step in a workflow — "
                "there is no processed text to translate yet.  "
                "Prepend a 'summarize' or 'custom_prompt' step."
            )
        return self


# ---------------------------------------------------------------------------
# Response Models
# ---------------------------------------------------------------------------


class WorkflowSubmitResponse(BaseModel):
    """Returned immediately after a valid workflow is accepted and enqueued."""

    task_id: str = Field(description="UUID to poll /workflow/{task_id}/status")
    status: WorkflowStatus = WorkflowStatus.QUEUED
    message: str = Field(description="Human-readable acceptance message")


class StepResult(BaseModel):
    """Execution result for a single workflow step — populated by the worker."""

    step_id: str
    status: StepStatus = StepStatus.PENDING
    output: str | None = Field(default=None, description="Text output produced by this step")
    error: str | None = Field(default=None, description="Error message if the step failed")
    started_at: datetime | None = None
    finished_at: datetime | None = None

    @property
    def duration_seconds(self) -> float | None:
        """Wall-clock time for this step, or None if not yet finished."""
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


class WorkflowStatusResponse(BaseModel):
    """
    Full status snapshot returned by GET /workflow/{task_id}/status.

    Clients should poll this until ``status`` is COMPLETED or FAILED.
    """

    task_id: str
    workflow_name: str
    status: WorkflowStatus
    step_results: list[StepResult] = Field(default_factory=list)
    final_output: str | None = Field(
        default=None,
        description="The output of the last successfully completed step",
    )
    submitted_at: datetime | None = None
    completed_at: datetime | None = None
    error: str | None = Field(default=None, description="Top-level error if the workflow failed")
