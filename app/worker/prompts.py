"""
AsyncFlow Engine — Dynamic Prompt Engineering (Phase 2).

Responsibility:
  Translate a typed ``WorkflowStep`` + ``input_text`` into a fully-rendered
  ``PromptPackage`` that ``LLMEngine`` can send directly to Ollama.

Design rationale:
  Keeping prompt templates here — separate from HTTP plumbing in ``engine.py``
  and orchestration logic in ``queue_tasks.py`` — means tuning model behaviour
  only ever requires changes to this file.  No risk of touching retry logic or
  transport code when tightening a system instruction.

Temperature policy:
  - EXTRACT_JSON / CLASSIFY: 0.0  — deterministic; creative variance is harmful
  - TRANSLATE:               0.2  — minor lexical flexibility, no hallucinations
  - SENTIMENT:               0.1  — near-deterministic, tiny room for nuance
  - SUMMARIZE:               0.3  — some paraphrase allowed, no creativity needed
  - CUSTOM_PROMPT:           user-set (clamped to [0.0, 2.0] by the schema)
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from app.core.config import settings
from app.core.schemas import (
    ClassifyConfig,
    CustomPromptConfig,
    ExtractJsonConfig,
    SentimentConfig,
    SummarizeConfig,
    TaskType,
    TranslateConfig,
    WorkflowStep,
)


# ---------------------------------------------------------------------------
# Public data contract
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PromptPackage:
    """
    Fully-rendered prompt ready for ``LLMEngine``.

    Attributes:
        system:        System instruction injected into Ollama's ``system`` field.
        user:          User turn — contains the (optionally templated) input text.
        use_json_mode: If ``True``, caller must set ``format="json"`` in the
                       Ollama payload to enforce structured output.
        temperature:   Sampling temperature (0.0 = deterministic).
        max_tokens:    Maximum tokens for the model to generate (``num_predict``).
    """

    system: str
    user: str
    use_json_mode: bool
    temperature: float
    max_tokens: int


# ---------------------------------------------------------------------------
# Per-task system instruction builders
# ---------------------------------------------------------------------------


def _summarize_prompt(config: SummarizeConfig, input_text: str) -> PromptPackage:
    """
    Build a summarisation prompt.

    Why no trailing preamble instruction?
      Llama 3.1 tends to start with "Here is a summary:" by default.
      The explicit "No preamble" in the system instruction suppresses this.
    """
    system = (
        f"You are an expert summariser. "
        f"Your output must be a {config.style} summary of at most {config.max_words} words. "
        f"Output ONLY the summary text — no headings, no preamble, no commentary."
    )
    return PromptPackage(
        system=system,
        user=f"Summarise the following text:\n\n{input_text}",
        use_json_mode=False,
        temperature=0.3,
        max_tokens=min(config.max_words * 6, settings.OLLAMA_MAX_TOKENS),
        # 6 chars/token is a conservative estimate — avoids hard truncation
    )


def _translate_prompt(config: TranslateConfig, input_text: str) -> PromptPackage:
    """
    Build a translation prompt.

    Why ``preserve_formatting`` handled here rather than engine-side?
      It is a semantic instruction to the model, not an API parameter.
      The engine only controls transport; prompt wording is this module's job.
    """
    formatting_note = (
        " Preserve all original formatting, line breaks, and structure."
        if config.preserve_formatting
        else ""
    )
    system = (
        f"You are a professional translator with native fluency in {config.target_language}. "
        f"Translate the input text into {config.target_language} ONLY.{formatting_note} "
        f"Output ONLY the translated text — no notes, no explanations, no alternatives."
    )
    return PromptPackage(
        system=system,
        user=f"Translate the following text into {config.target_language}:\n\n{input_text}",
        use_json_mode=False,
        temperature=0.2,
        max_tokens=settings.OLLAMA_MAX_TOKENS,
    )


def _extract_json_prompt(config: ExtractJsonConfig, input_text: str) -> PromptPackage:
    """
    Build a structured extraction prompt.

    Why temperature=0.0?
      JSON extraction is a deterministic parsing task.  Any temperature above
      zero introduces token sampling variance that can corrupt field names or
      values — exactly the failure mode that breaks downstream pipelines.

    Why inject the schema as pretty-printed JSON?
      The model has seen JSON Schema in training data in that format.  Compact
      single-line schema strings are less likely to be correctly interpreted.
    """
    schema_str = json.dumps(config.output_schema, indent=2)
    system = (
        "You are a precision data-extraction engine for enterprise pipelines. "
        "Extract information from the provided text and output it as a valid JSON object "
        "that STRICTLY conforms to the JSON Schema below.\n\n"
        f"```json\n{schema_str}\n```\n\n"
        "Rules:\n"
        "- Output ONLY the raw JSON object. No markdown fences, no explanation.\n"
        "- All required fields must be present.\n"
        "- Use null for fields where the source text provides no information.\n"
        "- Do NOT invent or hallucinate data that is not in the source text."
    )
    return PromptPackage(
        system=system,
        user=f"Extract structured data from the following text:\n\n{input_text}",
        use_json_mode=True,  # Instructs engine to set format="json"
        temperature=0.0,
        max_tokens=settings.OLLAMA_MAX_TOKENS,
    )


def _classify_prompt(config: ClassifyConfig, input_text: str) -> PromptPackage:
    """
    Build a classification prompt.

    Why temperature=0.0?
      Classification is a closed-set decision — we want the same input to
      always produce the same label.  Randomness is actively harmful here.

    Multi-label handling:
      The system instruction switches phrasing based on ``multi_label`` to avoid
      the model picking one label when multiple are valid.
    """
    labels_str = ", ".join(f'"{lbl}"' for lbl in config.labels)
    if config.multi_label:
        assignment = (
            f"Assign ALL applicable labels from this list: [{labels_str}]. "
            f"Output a comma-separated list of labels, ordered by relevance."
        )
    else:
        assignment = (
            f"Assign EXACTLY ONE label from this list: [{labels_str}]. "
            f"Output only the label text, nothing else."
        )
    system = (
        f"You are a text classification model. {assignment} "
        f"Do not explain your reasoning, do not add commentary."
    )
    return PromptPackage(
        system=system,
        user=f"Classify the following text:\n\n{input_text}",
        use_json_mode=False,
        temperature=0.0,
        max_tokens=128,  # Labels are short — no reason to generate more
    )


def _sentiment_prompt(config: SentimentConfig, input_text: str) -> PromptPackage:
    """
    Build a sentiment-analysis prompt.

    Output format is a deliberate structured convention (not JSON-mode) because:
      - JSON-mode adds overhead for a two-field response
      - The format "positive | 0.92" is easy to parse without a full JSON parser
      - This keeps the engine call path consistent with other non-JSON tasks
    """
    if config.granularity == "sentence":
        unit = "For EACH sentence, output one line"
        scope = "each sentence in the"
    else:
        unit = "Output a single line"
        scope = "the entire"

    system = (
        f"You are a sentiment analysis model. Analyse {scope} input text. "
        f"{unit} in this exact format: <sentiment> | <confidence>\n"
        f"Where <sentiment> is one of: positive, negative, neutral\n"
        f"And <confidence> is a decimal from 0.00 to 1.00.\n"
        f"Output ONLY the analysis lines — no headings, no preamble."
    )
    return PromptPackage(
        system=system,
        user=f"Analyse the sentiment of the following text:\n\n{input_text}",
        use_json_mode=False,
        temperature=0.1,
        max_tokens=256,
    )


def _custom_prompt(config: CustomPromptConfig, input_text: str) -> PromptPackage:
    """
    Build a prompt from the user-supplied template.

    Template contract:
      ``{input}`` is replaced with the chained input text.
      All other ``{...}`` placeholders are left for the model to interpret as
      literal text (we do a targeted single-key substitution, not format()).

    Why not ``str.format(**kwargs)``?
      If the user's template contains valid Python format specifiers (e.g.,
      ``{0}`` or ``{name}``), a full ``.format()`` call would raise KeyError.
      Replacing only ``{input}`` is the safe, minimal substitution.
    """
    rendered_prompt = config.prompt_template.replace("{input}", input_text)
    return PromptPackage(
        system="You are a helpful, precise assistant that follows instructions exactly.",
        user=rendered_prompt,
        use_json_mode=False,
        temperature=config.temperature,
        max_tokens=min(config.max_tokens, settings.OLLAMA_MAX_TOKENS),
    )


# ---------------------------------------------------------------------------
# Dispatch table (avoid if/elif chains — O(1) lookup)
# ---------------------------------------------------------------------------


_PROMPT_BUILDERS: dict[TaskType, Any] = {
    TaskType.SUMMARIZE: _summarize_prompt,
    TaskType.TRANSLATE: _translate_prompt,
    TaskType.EXTRACT_JSON: _extract_json_prompt,
    TaskType.CLASSIFY: _classify_prompt,
    TaskType.SENTIMENT: _sentiment_prompt,
    TaskType.CUSTOM_PROMPT: _custom_prompt,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_prompt(step: WorkflowStep, input_text: str) -> PromptPackage:
    """
    Construct a fully-rendered ``PromptPackage`` from a workflow step.

    This is the **only** function ``queue_tasks.py`` needs to call from this
    module.  All per-task logic is encapsulated in the builders above.

    Args:
        step:       The validated ``WorkflowStep`` including its typed config.
        input_text: The chained text input (previous step's output, or seed text).

    Returns:
        A ``PromptPackage`` ready to be passed directly to ``LLMEngine``.

    Raises:
        ValueError: If the step's ``task_type`` has no registered builder.
                    This should never happen if schemas and this module stay in sync.
    """
    task_type: TaskType = step.config.task_type
    builder = _PROMPT_BUILDERS.get(task_type)
    if builder is None:
        raise ValueError(
            f"No prompt builder registered for task_type='{task_type}'. "
            f"Register one in _PROMPT_BUILDERS in prompts.py."
        )
    return builder(step.config, input_text)
