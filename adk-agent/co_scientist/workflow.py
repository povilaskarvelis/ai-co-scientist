"""
ADK-native orchestration graph for the Co-Scientist agent.

This workflow keeps a flat agent graph (planner -> executor -> synthesizer),
but maintains structured step state in ADK session state via callbacks.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
from typing import Any

from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.agents.callback_context import CallbackContext
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.tools import McpToolset
from google.adk.tools.mcp_tool.mcp_toolset import StdioConnectionParams
from google.genai import types
from mcp.client.stdio import StdioServerParameters


MCP_SERVER_DIR = Path(__file__).resolve().parents[2] / "research-mcp"
DEFAULT_MODEL = os.getenv("ADK_NATIVE_MODEL", "gemini-2.5-flash")
HAS_BIGQUERY_RUNTIME_HINT = any(
    str(os.getenv(name, "")).strip()
    for name in ("BQ_PROJECT_ID", "BQ_DATASET_ALLOWLIST", "GOOGLE_CLOUD_PROJECT")
)
DEFAULT_PREFER_BIGQUERY = (
    str(os.getenv("ADK_NATIVE_PREFER_BIGQUERY", "1")).strip().lower() not in {"0", "false", "no"}
    and HAS_BIGQUERY_RUNTIME_HINT
)
BQ_PRIORITY_TOOLS = [
    "list_bigquery_tables",
    "run_bigquery_select_query",
]

STATE_WORKFLOW_TASK = "workflow_task_state"
STATE_WORKFLOW_TASK_LEGACY_APP = "app:workflow_task_state"
STATE_FINALIZE_REQUESTED = "temp:co_scientist_finalize_requested"
STATE_AUTO_SYNTH_REQUESTED = "temp:co_scientist_auto_synth_requested"
STATE_TURN_ABORT_REASON = "temp:co_scientist_turn_abort_reason"
STATE_PLANNER_BUFFER = "temp:co_scientist_planner_stream_buffer"
STATE_EXECUTOR_BUFFER = "temp:co_scientist_executor_stream_buffer"
STATE_SYNTH_BUFFER = "temp:co_scientist_synth_stream_buffer"
STATE_PLANNER_RENDERED = "temp:co_scientist_planner_rendered"
STATE_EXECUTOR_RENDERED = "temp:co_scientist_executor_rendered"
STATE_EXECUTOR_ACTIVE_STEP_ID = "temp:co_scientist_executor_active_step_id"
STATE_EXECUTOR_PREV_STEP_STATUS = "temp:co_scientist_executor_prev_step_status"

FINALIZE_COMMANDS = {
    "finalize",
    "summarize now",
    "final summary",
    "/finalize",
}

PLAN_SCHEMA = "plan_internal.v1"
STEP_RESULT_SCHEMA = "step_execution_result.v1"
EXECUTION_BATCH_SCHEMA = "execution_batch_result.v1"
FINAL_SYNTHESIS_SCHEMA = "final_synthesis.v1"
WORKFLOW_TASK_SCHEMA = "workflow_task_state.v1"

KNOWN_MCP_TOOLS = [
    "list_bigquery_tables",
    "run_bigquery_select_query",
    "benchmark_dataset_overview",
    "sample_pubmedqa_examples",
    "sample_bioasq_examples",
    "check_gpqa_access",
    "search_diseases",
    "expand_disease_context",
    "search_targets",
    "search_disease_targets",
    "get_target_info",
    "check_druggability",
    "get_target_drugs",
    "summarize_target_expression_context",
    "summarize_target_competitive_landscape",
    "summarize_target_safety_liabilities",
    "compare_targets_multi_axis",
    "search_clinical_trials",
    "get_clinical_trial",
    "summarize_clinical_trials_landscape",
    "search_pubmed",
    "search_pubmed_advanced",
    "get_pubmed_abstract",
    "get_pubmed_paper_details",
    "get_pubmed_author_profile",
    "search_openalex_works",
    "search_openalex_authors",
    "rank_researchers_by_activity",
    "get_researcher_contact_candidates",
    "search_chembl_compounds_for_target",
    "search_gwas_associations",
    "infer_genetic_effect_direction",
    "search_clinvar_variants",
    "get_clinvar_variant_details",
    "search_reactome_pathways",
    "get_string_interactions",
    "get_gene_info",
    "list_local_datasets",
    "read_local_dataset",
]


PLANNER_INSTRUCTION_TEMPLATE = """
You are the internal planner for biomedical investigation.

Available MCP tools:
__TOOL_CATALOG__

Rules:
- Build a concrete execution plan before any evidence collection begins.
- Break the objective into ordered, atomic subtasks.
- Prioritize high-signal subtasks that reduce uncertainty first.
- Choose the number of steps needed for the objective. Avoid unnecessary fragmentation.
- Each step must include: id, goal, tool_hint, completion_condition.
- Use step ids S1, S2, S3, ... in order.
- Do not call tools.

__BQ_POLICY__

Output requirements:
- Return ONLY valid JSON (no markdown, no prose) matching this shape:
  {
    "schema": "plan_internal.v1",
    "objective": "<restated objective>",
    "success_criteria": ["..."],
    "steps": [
      {
        "id": "S1",
        "goal": "...",
        "tool_hint": "<single best tool or tool family>",
        "completion_condition": "..."
      }
    ]
  }
"""


EVIDENCE_EXECUTOR_INSTRUCTION_TEMPLATE = """
You execute biomedical evidence collection and validation.
Use a ReAct-style workflow internally: decide the next action, use MCP tools, observe results, and reassess.

Available MCP tools:
__TOOL_CATALOG__

Rules:
- Execute the pending plan steps provided in the injected execution context, in order.
- Continue through all pending steps in this turn unless a step is blocked.
- If a step is blocked, stop further planned steps and report the blocked step plus completed prior step results.
- Use MCP tools only when they directly improve evidence quality.
- Prioritize high-signal evidence before broad expansion.
- Surface contradictions and unresolved gaps explicitly.
- Include source identifiers when available (PMID, DOI, NCT, OpenAlex IDs).
__BQ_POLICY__

Output requirements:
- Return ONLY valid JSON (no markdown, no prose) matching this shape:
  {
    "schema": "execution_batch_result.v1",
    "step_results": [
      {
        "step_id": "S1",
        "status": "completed" | "blocked",
        "step_progress_note": "<1-2 sentence progress update>",
        "result_summary": "<concise findings summary>",
        "evidence_ids": ["PMID:...", "NCT:..."],
        "open_gaps": ["..."],
        "suggested_next_searches": ["..."]
      }
    ]
  }
"""


SYNTHESIZER_INSTRUCTION = """
You are the final biomedical report synthesizer.
You will receive structured state context (objective, plan steps, step results, and coverage status).

Rules:
- Produce a final summary grounded only in the provided evidence/results.
- If the plan is incomplete, clearly state that the summary is partial.
- Do not invent unsupported claims.
- Avoid terse output. Be specific and useful.
- For each supporting evidence item, include:
  - the evidence-backed claim,
  - why it matters for the objective (rationale),
  - source identifiers when available.
- Always include potential next steps, even when the plan is complete (e.g., confirmatory checks, risk reduction, monitoring, or decision-oriented follow-up).

Output requirements:
- Return user-facing Markdown.
- Include these sections:
  - `## Final Summary`
  - `Supporting Evidence`
  - `Limitations`
  - `Potential Next Steps`
"""


def _dedupe_str_list(values: list[Any], *, limit: int = 20) -> list[str]:
    cleaned: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = re.sub(r"\s+", " ", str(value or "").strip())
        if not item:
            continue
        lowered = item.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        cleaned.append(item)
        if len(cleaned) >= max(1, limit):
            break
    return cleaned


def _normalize_user_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip()).lower()


def _is_finalize_command(text: str) -> bool:
    return _normalize_user_text(text) in FINALIZE_COMMANDS


def _extract_user_turn_text(callback_context: CallbackContext) -> str:
    user_content = getattr(callback_context, "user_content", None)
    parts = getattr(user_content, "parts", None) if user_content is not None else None
    if not parts:
        return ""
    text = " ".join(
        str(getattr(part, "text", "") or "").strip()
        for part in parts
        if str(getattr(part, "text", "") or "").strip()
    )
    return re.sub(r"\s+", " ", text).strip()


def _make_content(text: str) -> types.Content:
    return types.Content(role="model", parts=[types.Part.from_text(text=text)])


def _make_text_response(text: str) -> LlmResponse:
    return LlmResponse(content=_make_content(text), partial=False, turn_complete=True)


def _replace_llm_response_text(llm_response: LlmResponse, text: str) -> LlmResponse:
    updated = llm_response.model_copy(deep=True)
    updated.content = _make_content(text)
    return updated


def _llm_response_text(llm_response: LlmResponse) -> str:
    content = getattr(llm_response, "content", None)
    parts = getattr(content, "parts", None) if content is not None else None
    if not parts:
        return ""
    return "".join(
        str(getattr(part, "text", "") or "")
        for part in parts
        if isinstance(getattr(part, "text", None), str)
    )


def _llm_response_has_function_call(llm_response: LlmResponse) -> bool:
    content = getattr(llm_response, "content", None)
    parts = getattr(content, "parts", None) if content is not None else None
    if not parts:
        return False
    return any(getattr(part, "function_call", None) is not None for part in parts)


def _set_temp_state(callback_context: CallbackContext, key: str, value: Any) -> None:
    callback_context.state[key] = value


def _get_temp_state(callback_context: CallbackContext, key: str, default: Any = None) -> Any:
    return callback_context.state.get(key, default)


def _clear_turn_temp_state(callback_context: CallbackContext) -> None:
    # Cleanup legacy app-scoped workflow state that caused cross-session bleed.
    if STATE_WORKFLOW_TASK_LEGACY_APP in callback_context.state:
        callback_context.state[STATE_WORKFLOW_TASK_LEGACY_APP] = None
    callback_context.state[STATE_AUTO_SYNTH_REQUESTED] = False
    callback_context.state[STATE_TURN_ABORT_REASON] = ""
    callback_context.state[STATE_PLANNER_BUFFER] = ""
    callback_context.state[STATE_EXECUTOR_BUFFER] = ""
    callback_context.state[STATE_SYNTH_BUFFER] = ""
    callback_context.state[STATE_PLANNER_RENDERED] = ""
    callback_context.state[STATE_EXECUTOR_RENDERED] = ""
    callback_context.state[STATE_EXECUTOR_ACTIVE_STEP_ID] = ""
    callback_context.state[STATE_EXECUTOR_PREV_STEP_STATUS] = ""


def _set_turn_rendered_output(callback_context: CallbackContext, *, key: str, text: str) -> None:
    callback_context.state[key] = str(text or "")


def _compose_non_finalize_turn_output(callback_context: CallbackContext) -> str:
    planner_text = str(callback_context.state.get(STATE_PLANNER_RENDERED, "") or "").strip()
    executor_text = str(callback_context.state.get(STATE_EXECUTOR_RENDERED, "") or "").strip()
    parts = [part for part in (planner_text, executor_text) if part]
    return "\n\n".join(parts).strip()


def _json_candidate_from_fenced_block(text: str) -> str | None:
    match = re.search(r"```(?:json)?\s*([\[{].*[\]}])\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip()


def _extract_balanced_json_substring(text: str) -> str | None:
    start = None
    opening = ""
    closing = ""
    depth = 0
    in_string = False
    escape = False
    for idx, ch in enumerate(text):
        if start is None:
            if ch == "{":
                start = idx
                opening = "{"
                closing = "}"
                depth = 1
            elif ch == "[":
                start = idx
                opening = "["
                closing = "]"
                depth = 1
            continue

        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == opening:
            depth += 1
        elif ch == closing:
            depth -= 1
            if depth == 0:
                return text[start : idx + 1].strip()
    return None


def _parse_json_object_from_text(raw_text: str) -> tuple[dict[str, Any] | None, str | None]:
    raw = str(raw_text or "").strip()
    if not raw:
        return None, "Empty model output."

    candidates: list[str] = []
    for candidate in (
        raw,
        _json_candidate_from_fenced_block(raw),
        _extract_balanced_json_substring(raw),
    ):
        if not candidate:
            continue
        if candidate in candidates:
            continue
        candidates.append(candidate)

    last_error = "Failed to parse JSON object."
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception as exc:  # noqa: BLE001
            last_error = f"JSON parse error: {exc}"
            continue
        if not isinstance(parsed, dict):
            last_error = "Top-level JSON value must be an object."
            continue
        return parsed, None
    return None, last_error


def _buffer_partial_text(callback_context: CallbackContext, buffer_key: str, chunk: str) -> None:
    if not chunk:
        return
    existing = str(callback_context.state.get(buffer_key, "") or "")
    callback_context.state[buffer_key] = existing + chunk


def _consume_buffered_json_object(
    callback_context: CallbackContext,
    *,
    buffer_key: str,
    llm_response: LlmResponse,
) -> tuple[dict[str, Any] | None, str | None]:
    current_text = _llm_response_text(llm_response)
    buffered = str(callback_context.state.get(buffer_key, "") or "")
    callback_context.state[buffer_key] = ""

    candidates: list[str] = []
    for candidate in (
        current_text,
        buffered + current_text,
        buffered,
    ):
        candidate = str(candidate or "")
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    if not candidates:
        return None, "Empty model output."

    last_error = "Failed to parse JSON object."
    for candidate in candidates:
        parsed, err = _parse_json_object_from_text(candidate)
        if parsed is not None:
            return parsed, None
        if err:
            last_error = err
    return None, last_error


def _as_nonempty_str(value: Any, field_name: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text:
        raise ValueError(f"{field_name} must be a non-empty string")
    return text


def _as_string_list(value: Any, field_name: str, *, limit: int = 20) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return _dedupe_str_list(value, limit=limit)


def _validate_plan_internal(raw: dict[str, Any]) -> dict[str, Any]:
    if str(raw.get("schema", "")).strip() != PLAN_SCHEMA:
        raise ValueError(f"schema must be {PLAN_SCHEMA}")
    objective = _as_nonempty_str(raw.get("objective"), "objective")
    success_criteria = _as_string_list(raw.get("success_criteria"), "success_criteria", limit=8)
    if not success_criteria:
        raise ValueError("success_criteria must contain at least one item")

    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list):
        raise ValueError("steps must be a list")
    if len(steps_raw) < 1:
        raise ValueError("steps must contain at least one item")

    steps: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for idx, step in enumerate(steps_raw, start=1):
        if not isinstance(step, dict):
            raise ValueError(f"steps[{idx - 1}] must be an object")
        step_id = _as_nonempty_str(step.get("id"), f"steps[{idx - 1}].id")
        if step_id != f"S{idx}":
            raise ValueError(f"steps[{idx - 1}].id must be S{idx}")
        if step_id in seen_ids:
            raise ValueError(f"Duplicate step id: {step_id}")
        seen_ids.add(step_id)
        steps.append(
            {
                "id": step_id,
                "goal": _as_nonempty_str(step.get("goal"), f"steps[{idx - 1}].goal"),
                "tool_hint": _as_nonempty_str(step.get("tool_hint"), f"steps[{idx - 1}].tool_hint"),
                "completion_condition": _as_nonempty_str(
                    step.get("completion_condition"),
                    f"steps[{idx - 1}].completion_condition",
                ),
            }
        )

    return {
        "schema": PLAN_SCHEMA,
        "objective": objective,
        "success_criteria": success_criteria,
        "steps": steps,
    }


def _initialize_task_state_from_plan(plan: dict[str, Any], *, objective_text: str) -> dict[str, Any]:
    validated = _validate_plan_internal(plan)
    steps = [
        {
            "id": step["id"],
            "goal": step["goal"],
            "tool_hint": step["tool_hint"],
            "completion_condition": step["completion_condition"],
            "status": "pending",
            "result_summary": "",
            "evidence_ids": [],
            "open_gaps": [],
            "suggested_next_searches": [],
            "step_progress_note": "",
        }
        for step in validated["steps"]
    ]
    return {
        "schema": WORKFLOW_TASK_SCHEMA,
        "objective": objective_text or validated["objective"],
        "objective_fingerprint": _normalize_user_text(objective_text or validated["objective"]),
        "plan_status": "ready",
        "current_step_id": steps[0]["id"] if steps else None,
        "last_completed_step_id": None,
        "steps": steps,
        "success_criteria": validated["success_criteria"],
        "latest_synthesis": None,
    }


def _get_task_state(callback_context: CallbackContext) -> dict[str, Any] | None:
    state = callback_context.state.get(STATE_WORKFLOW_TASK)
    return state if isinstance(state, dict) else None


def _find_step(task_state: dict[str, Any], step_id: str) -> tuple[int, dict[str, Any]]:
    for idx, step in enumerate(task_state.get("steps", [])):
        if str(step.get("id")) == str(step_id):
            return idx, step
    raise ValueError(f"Step not found: {step_id}")


def _next_pending_step_id(task_state: dict[str, Any]) -> str | None:
    for step in task_state.get("steps", []):
        if str(step.get("status", "")) == "pending":
            return str(step.get("id"))
    return None


def _completed_step_count(task_state: dict[str, Any]) -> int:
    return sum(1 for step in task_state.get("steps", []) if str(step.get("status")) == "completed")


def _total_step_count(task_state: dict[str, Any]) -> int:
    return len(task_state.get("steps", []))


def _compute_coverage_status(task_state: dict[str, Any]) -> str:
    steps = task_state.get("steps", [])
    if steps and all(str(step.get("status")) == "completed" for step in steps):
        return "complete_plan"
    return "partial_plan"


def _compact_completed_step_summaries(task_state: dict[str, Any]) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for step in task_state.get("steps", []):
        if str(step.get("status")) != "completed":
            continue
        summaries.append(
            {
                "id": step.get("id"),
                "goal": step.get("goal"),
                "result_summary": step.get("result_summary", ""),
                "evidence_ids": list(step.get("evidence_ids", []) or [])[:20],
                "open_gaps": list(step.get("open_gaps", []) or [])[:10],
            }
        )
    return summaries


def _serialize_pretty_json(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=True)


def _render_plan_markdown(task_state: dict[str, Any]) -> str:
    lines = ["## Plan", ""]
    objective = str(task_state.get("objective", "")).strip()
    if objective:
        lines.append(f"**Objective:** {objective}")
        lines.append("")
    for step in task_state.get("steps", []):
        lines.append(
            f"1. **{step.get('id', 'S?')}**: {step.get('goal', '').strip()} "
            f"(tool: `{step.get('tool_hint', '').strip()}`)"
        )
        lines.append(f"Completion: {step.get('completion_condition', '').strip()}")
    return "\n".join(lines).strip()


def _render_executor_progress_markdown(task_state: dict[str, Any], step_result: dict[str, Any]) -> str:
    step_id = str(step_result.get("step_id", "")).strip()
    _, step = _find_step(task_state, step_id)
    status = str(step.get("status", "")).strip()
    lines = [f"## Step {step_id}", ""]
    lines.append(f"**Goal:** {step.get('goal', '').strip()}")
    lines.append(f"**Status:** `{status}`")
    progress_note = str(step.get("step_progress_note", "")).strip()
    if progress_note:
        lines.append("")
        lines.append(progress_note)
    result_summary = str(step.get("result_summary", "")).strip()
    if result_summary:
        lines.append("")
        lines.append("**Key Findings**")
        lines.append(result_summary)
    evidence_ids = [str(x).strip() for x in step.get("evidence_ids", []) if str(x).strip()]
    if evidence_ids:
        lines.append("")
        lines.append("**Evidence IDs**")
        lines.extend(f"- `{eid}`" for eid in evidence_ids[:20])
    open_gaps = [str(x).strip() for x in step.get("open_gaps", []) if str(x).strip()]
    if open_gaps:
        lines.append("")
        lines.append("**Open Gaps / Uncertainty**")
        lines.extend(f"- {gap}" for gap in open_gaps[:10])
    next_searches = [str(x).strip() for x in step.get("suggested_next_searches", []) if str(x).strip()]
    if next_searches:
        lines.append("")
        lines.append("**Suggested Next Searches / Tool Calls**")
        lines.extend(f"- {item}" for item in next_searches[:10])

    completed = _completed_step_count(task_state)
    total = _total_step_count(task_state)
    next_step_id = task_state.get("current_step_id")
    footer = f"Completed {completed}/{total} steps"
    if next_step_id:
        footer += f"; next: {next_step_id}"
    elif str(task_state.get("plan_status", "")) == "completed":
        footer += "; all planned steps complete — reply `finalize` for a final summary"
    lines.append("")
    lines.append(f"_Progress: {footer}_")
    return "\n".join(lines).strip()


def _render_executor_batch_progress_markdown(task_state: dict[str, Any], step_results: list[dict[str, Any]]) -> str:
    lines = ["## Execution Progress", ""]
    for result in step_results:
        step_id = str(result.get("step_id", "")).strip()
        try:
            _, step = _find_step(task_state, step_id)
        except Exception:  # noqa: BLE001
            step = {}
        status = str(result.get("status", step.get("status", ""))).strip()
        goal = str(step.get("goal", "")).strip()
        lines.append(f"### {step_id} · `{status}`")
        if goal:
            lines.append(f"**Goal:** {goal}")
        progress_note = str(result.get("step_progress_note", "")).strip()
        if progress_note:
            lines.append(progress_note)
        result_summary = str(result.get("result_summary", "")).strip()
        if result_summary:
            lines.append(f"**Findings:** {result_summary}")
        evidence_ids = [str(x).strip() for x in result.get("evidence_ids", []) if str(x).strip()]
        if evidence_ids:
            lines.append("**Evidence IDs:** " + ", ".join(f"`{eid}`" for eid in evidence_ids[:8]))
        open_gaps = [str(x).strip() for x in result.get("open_gaps", []) if str(x).strip()]
        if open_gaps:
            lines.append("**Open gaps:** " + "; ".join(open_gaps[:4]))
        next_searches = [str(x).strip() for x in result.get("suggested_next_searches", []) if str(x).strip()]
        if next_searches:
            lines.append("**Suggested next:** " + "; ".join(next_searches[:4]))
        lines.append("")

    completed = _completed_step_count(task_state)
    total = _total_step_count(task_state)
    if str(task_state.get("plan_status", "")) == "completed":
        footer = f"Completed {completed}/{total} steps; generating final summary."
    elif str(task_state.get("plan_status", "")) == "blocked":
        footer = f"Completed {completed}/{total} steps; blocked at {task_state.get('current_step_id')}"
    else:
        footer = f"Completed {completed}/{total} steps; next: {task_state.get('current_step_id')}"
    lines.append(f"_Progress: {footer}_")
    return "\n".join(line for line in lines if line is not None).strip()


def _fallback_supporting_evidence_from_task_state(task_state: dict[str, Any]) -> list[str]:
    items: list[str] = []
    for step in task_state.get("steps", []):
        if str(step.get("status", "")).strip() != "completed":
            continue
        goal = str(step.get("goal", "")).strip()
        summary = str(step.get("result_summary", "")).strip()
        if not summary:
            continue
        evidence_ids = [str(x).strip() for x in step.get("evidence_ids", []) if str(x).strip()]
        evidence_suffix = f" (IDs: {', '.join(evidence_ids[:6])})" if evidence_ids else ""
        if goal:
            items.append(f"{summary} This matters because it addresses the step goal: {goal}.{evidence_suffix}")
        else:
            items.append(summary + evidence_suffix)
        if len(items) >= 8:
            break
    return items


def _fallback_next_actions_from_task_state(task_state: dict[str, Any]) -> list[str]:
    actions: list[str] = []
    plan_status = str(task_state.get("plan_status", "")).strip()
    current_step_id = str(task_state.get("current_step_id") or "").strip()
    if plan_status == "blocked" and current_step_id:
        actions.append(f"Unblock and rerun {current_step_id} to complete the planned evidence collection.")
    elif plan_status != "completed":
        pending_steps = [
            str(step.get("id"))
            for step in task_state.get("steps", [])
            if str(step.get("status", "")).strip() == "pending"
        ]
        if pending_steps:
            actions.append(f"Continue the remaining planned steps in order ({', '.join(pending_steps[:6])}).")

    # Convert open gaps into concrete follow-up actions.
    seen_gaps: set[str] = set()
    for step in task_state.get("steps", []):
        for gap in step.get("open_gaps", []) or []:
            gap_text = re.sub(r"\s+", " ", str(gap or "").strip())
            if not gap_text:
                continue
            low = gap_text.lower()
            if low in seen_gaps:
                continue
            seen_gaps.add(low)
            actions.append(f"Address open gap: {gap_text}")
            if len(actions) >= 5:
                return actions

    if not actions:
        actions.append("Review the compiled evidence for decision readiness and identify any confirmatory analyses worth running.")
        actions.append("Document confidence level and assumptions before making a downstream decision or recommendation.")
    return actions[:5]


def _coverage_note_from_task_state(task_state: dict[str, Any]) -> str:
    coverage = _compute_coverage_status(task_state)
    completed = _completed_step_count(task_state)
    total = _total_step_count(task_state)
    coverage_label = "Complete plan" if coverage == "complete_plan" else "Partial plan"
    if coverage == "partial_plan":
        return (
            f"_Coverage: {coverage_label} ({completed} of {total} planned steps completed when final summary was requested)._"
        )
    return f"_Coverage: {coverage_label} ({completed} of {total} planned steps completed)._"


def _postprocess_synth_markdown(task_state: dict[str, Any], raw_markdown: str) -> str:
    text = str(raw_markdown or "").strip()
    if not text:
        text = "## Final Summary\n\nNo final summary was produced."

    if "## Final Summary" not in text and "# Final Summary" not in text:
        text = "## Final Summary\n\n" + text

    lowered = text.lower()
    if "supporting evidence" not in lowered:
        fallback_supporting = _fallback_supporting_evidence_from_task_state(task_state)
        if fallback_supporting:
            text += "\n\n**Supporting Evidence (Claim + Why It Matters)**\n"
            text += "\n".join(f"- {item}" for item in fallback_supporting[:20])

    if "potential next steps" not in lowered and "next steps" not in lowered and "next actions" not in lowered:
        fallback_next = _fallback_next_actions_from_task_state(task_state)
        if fallback_next:
            text += "\n\n**Potential Next Steps**\n"
            text += "\n".join(f"- {item}" for item in fallback_next[:20])

    if "_coverage:" not in lowered and "coverage:" not in lowered:
        text += "\n\n" + _coverage_note_from_task_state(task_state)

    return text.strip()


def _render_final_synthesis_markdown(task_state: dict[str, Any], synthesis: dict[str, Any]) -> str:
    lines = ["## Final Summary", ""]
    direct_answer = str(synthesis.get("direct_answer", "")).strip()
    if direct_answer:
        lines.append(direct_answer)
    supporting = [str(x).strip() for x in synthesis.get("supporting_evidence", []) if str(x).strip()]
    if not supporting:
        supporting = _fallback_supporting_evidence_from_task_state(task_state)
    if supporting:
        lines.append("")
        lines.append("**Supporting Evidence (Claim + Why It Matters)**")
        lines.extend(f"- {item}" for item in supporting[:20])
    limitations = [str(x).strip() for x in synthesis.get("limitations", []) if str(x).strip()]
    if limitations:
        lines.append("")
        lines.append("**Limitations**")
        lines.extend(f"- {item}" for item in limitations[:20])
    next_actions = [str(x).strip() for x in synthesis.get("next_actions", []) if str(x).strip()]
    if not next_actions:
        next_actions = _fallback_next_actions_from_task_state(task_state)
    if next_actions:
        lines.append("")
        lines.append("**Potential Next Steps**")
        lines.extend(f"- {item}" for item in next_actions[:20])

    coverage = str(synthesis.get("coverage_status", "partial_plan"))
    completed = _completed_step_count(task_state)
    total = _total_step_count(task_state)
    coverage_label = "Complete plan" if coverage == "complete_plan" else "Partial plan"
    if coverage == "partial_plan":
        lines.append("")
        lines.append(
            f"_Coverage: {coverage_label} ({completed} of {total} planned steps completed when final summary was requested)._"
        )
    else:
        lines.append("")
        lines.append(f"_Coverage: {coverage_label} ({completed} of {total} planned steps completed)._")
    return "\n".join(lines).strip()


def _render_parse_error_markdown(agent_label: str, error: str, raw_excerpt: str) -> str:
    excerpt = re.sub(r"\s+", " ", raw_excerpt).strip()
    if len(excerpt) > 240:
        excerpt = excerpt[:237] + "..."
    lines = [f"## {agent_label} Parse Error", "", f"{error}"]
    if excerpt:
        lines.append("")
        lines.append(f"Raw output excerpt: `{excerpt}`")
    return "\n".join(lines).strip()


def _render_no_plan_to_finalize_message() -> str:
    return (
        "## Final Summary\n\n"
        "No plan or collected evidence is available yet. Ask a research question first, "
        "then use `finalize` when you want a final summary."
    )


def _render_all_steps_complete_message() -> str:
    return (
        "## Execution\n\n"
        "All planned steps are already complete. Reply `finalize` to generate the final summary."
    )


def _planner_json_instruction_suffix() -> str:
    return (
        "Return ONLY valid JSON matching `plan_internal.v1` for this objective. "
        "Do not include markdown fences or commentary."
    )


def _executor_context_instructions(task_state: dict[str, Any], active_step: dict[str, Any]) -> list[str]:
    prior_completed = _compact_completed_step_summaries(task_state)
    pending_steps = [
        {
            "id": step.get("id"),
            "goal": step.get("goal"),
            "tool_hint": step.get("tool_hint"),
            "completion_condition": step.get("completion_condition"),
        }
        for step in task_state.get("steps", [])
        if str(step.get("status", "")).strip() in {"pending", "in_progress"}
    ]
    payload = {
        "schema": "executor_plan_context.v1",
        "objective": task_state.get("objective", ""),
        "plan_status": task_state.get("plan_status", "ready"),
        "active_step": {
            "id": active_step.get("id"),
            "goal": active_step.get("goal"),
            "tool_hint": active_step.get("tool_hint"),
            "completion_condition": active_step.get("completion_condition"),
        },
        "pending_steps": pending_steps,
        "prior_completed_steps": prior_completed,
    }
    return [
        "Execution context (authoritative; use this instead of inferring from prior prose):",
        _serialize_pretty_json(payload),
        (
            "Return ONLY valid JSON matching `execution_batch_result.v1`. "
            "Do not include markdown fences or extra commentary."
        ),
    ]


def _synth_context_instructions(task_state: dict[str, Any]) -> list[str]:
    payload = {
        "schema": "synthesis_context.v1",
        "objective": task_state.get("objective", ""),
        "plan_status": task_state.get("plan_status", "ready"),
        "coverage_status": _compute_coverage_status(task_state),
        "steps": [
            {
                "id": step.get("id"),
                "goal": step.get("goal"),
                "status": step.get("status"),
                "result_summary": step.get("result_summary", ""),
                "evidence_ids": list(step.get("evidence_ids", []) or [])[:20],
                "open_gaps": list(step.get("open_gaps", []) or [])[:10],
            }
            for step in task_state.get("steps", [])
        ],
    }
    return [
        "Synthesis context (authoritative; use this instead of inferring from prior prose):",
        _serialize_pretty_json(payload),
        (
            "Return user-facing Markdown (not JSON). Include a direct answer, supporting evidence with rationale, "
            "limitations, potential next steps, and a coverage note (complete vs partial plan)."
        ),
    ]


def _validate_step_execution_result(raw: dict[str, Any]) -> dict[str, Any]:
    if str(raw.get("schema", "")).strip() != STEP_RESULT_SCHEMA:
        raise ValueError(f"schema must be {STEP_RESULT_SCHEMA}")
    status = str(raw.get("status", "")).strip().lower()
    if status not in {"completed", "blocked"}:
        raise ValueError("status must be `completed` or `blocked`")
    return {
        "schema": STEP_RESULT_SCHEMA,
        "step_id": _as_nonempty_str(raw.get("step_id"), "step_id"),
        "status": status,
        "step_progress_note": _as_nonempty_str(raw.get("step_progress_note"), "step_progress_note"),
        "result_summary": _as_nonempty_str(raw.get("result_summary"), "result_summary"),
        "evidence_ids": _as_string_list(raw.get("evidence_ids"), "evidence_ids", limit=30),
        "open_gaps": _as_string_list(raw.get("open_gaps"), "open_gaps", limit=15),
        "suggested_next_searches": _as_string_list(
            raw.get("suggested_next_searches"),
            "suggested_next_searches",
            limit=15,
        ),
    }


def _validate_execution_batch_result(raw: dict[str, Any]) -> dict[str, Any]:
    if str(raw.get("schema", "")).strip() != EXECUTION_BATCH_SCHEMA:
        raise ValueError(f"schema must be {EXECUTION_BATCH_SCHEMA}")
    step_results_raw = raw.get("step_results")
    if not isinstance(step_results_raw, list) or not step_results_raw:
        raise ValueError("step_results must be a non-empty list")
    validated_results: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    blocked_seen = False
    for idx, item in enumerate(step_results_raw):
        if not isinstance(item, dict):
            raise ValueError(f"step_results[{idx}] must be an object")
        validated = _validate_step_execution_result({"schema": STEP_RESULT_SCHEMA, **item})
        step_id = validated["step_id"]
        if step_id in seen_ids:
            raise ValueError(f"Duplicate step_id in step_results: {step_id}")
        if blocked_seen:
            raise ValueError("No step results are allowed after a blocked step")
        if validated["status"] == "blocked":
            blocked_seen = True
        seen_ids.add(step_id)
        validated_results.append(validated)
    return {"schema": EXECUTION_BATCH_SCHEMA, "step_results": validated_results}


def _apply_step_execution_result_to_task_state(
    task_state: dict[str, Any],
    step_result: dict[str, Any],
) -> dict[str, Any]:
    validated = _validate_step_execution_result(step_result)
    current_step_id = str(task_state.get("current_step_id") or "").strip()
    if current_step_id and validated["step_id"] != current_step_id:
        raise ValueError(
            f"Executor returned step_id {validated['step_id']} but active step is {current_step_id}"
        )

    _, step = _find_step(task_state, validated["step_id"])
    step["status"] = validated["status"]
    step["step_progress_note"] = validated["step_progress_note"]
    step["result_summary"] = validated["result_summary"]
    step["evidence_ids"] = validated["evidence_ids"]
    step["open_gaps"] = validated["open_gaps"]
    step["suggested_next_searches"] = validated["suggested_next_searches"]

    if validated["status"] == "completed":
        task_state["last_completed_step_id"] = validated["step_id"]
        next_step_id = _next_pending_step_id(task_state)
        task_state["current_step_id"] = next_step_id
        task_state["plan_status"] = "completed" if next_step_id is None else "ready"
    else:
        task_state["current_step_id"] = validated["step_id"]
        task_state["plan_status"] = "blocked"

    return validated


def _apply_execution_batch_result_to_task_state(
    task_state: dict[str, Any],
    batch_result: dict[str, Any],
) -> list[dict[str, Any]]:
    validated_batch = _validate_execution_batch_result(batch_result)
    pending_ids = [
        str(step.get("id"))
        for step in task_state.get("steps", [])
        if str(step.get("status", "")).strip() in {"pending", "in_progress"}
    ]
    result_ids = [result["step_id"] for result in validated_batch["step_results"]]
    if not result_ids:
        raise ValueError("step_results must contain at least one step result")
    if pending_ids[: len(result_ids)] != result_ids:
        raise ValueError(
            f"step_results must follow pending step order. Expected prefix {pending_ids[:len(result_ids)]}, got {result_ids}"
        )

    applied_results: list[dict[str, Any]] = []
    for result in validated_batch["step_results"]:
        # Temporarily align current_step_id so existing single-step apply validation remains correct.
        task_state["current_step_id"] = result["step_id"]
        applied = _apply_step_execution_result_to_task_state(task_state, {"schema": STEP_RESULT_SCHEMA, **result})
        applied_results.append(applied)
        if applied["status"] == "blocked":
            break
    return applied_results


def _validate_final_synthesis(raw: dict[str, Any]) -> dict[str, Any]:
    if str(raw.get("schema", "")).strip() != FINAL_SYNTHESIS_SCHEMA:
        raise ValueError(f"schema must be {FINAL_SYNTHESIS_SCHEMA}")
    mode = str(raw.get("mode", "")).strip().lower()
    if mode != "final":
        raise ValueError("mode must be `final`")
    coverage_status = str(raw.get("coverage_status", "")).strip().lower()
    if coverage_status not in {"complete_plan", "partial_plan"}:
        raise ValueError("coverage_status must be `complete_plan` or `partial_plan`")
    return {
        "schema": FINAL_SYNTHESIS_SCHEMA,
        "mode": "final",
        "coverage_status": coverage_status,
        "direct_answer": _as_nonempty_str(raw.get("direct_answer"), "direct_answer"),
        "supporting_evidence": _as_string_list(raw.get("supporting_evidence"), "supporting_evidence", limit=30),
        "limitations": _as_string_list(raw.get("limitations"), "limitations", limit=20),
        "next_actions": _as_string_list(raw.get("next_actions"), "next_actions", limit=20),
    }


def _planner_before_model_callback(*, callback_context: CallbackContext, llm_request: LlmRequest) -> LlmResponse | None:
    _clear_turn_temp_state(callback_context)
    user_text = _extract_user_turn_text(callback_context)
    is_finalize = _is_finalize_command(user_text)
    callback_context.state[STATE_FINALIZE_REQUESTED] = bool(is_finalize)

    if is_finalize:
        return _make_text_response("")

    task_state = _get_task_state(callback_context)
    normalized = _normalize_user_text(user_text)
    if task_state and str(task_state.get("objective_fingerprint", "")) == normalized:
        return _make_text_response("")

    if task_state and normalized and str(task_state.get("objective_fingerprint", "")) != normalized:
        callback_context.state[STATE_WORKFLOW_TASK] = None

    llm_request.config = llm_request.config or types.GenerateContentConfig()
    llm_request.config.response_mime_type = "application/json"
    llm_request.append_instructions([_planner_json_instruction_suffix()])
    return None


def _planner_after_model_callback(*, callback_context: CallbackContext, llm_response: LlmResponse) -> LlmResponse | None:
    if _llm_response_has_function_call(llm_response):
        return None

    text = _llm_response_text(llm_response)
    if bool(getattr(llm_response, "partial", False)):
        _buffer_partial_text(callback_context, STATE_PLANNER_BUFFER, text)
        return _replace_llm_response_text(llm_response, "")

    parsed, parse_error = _consume_buffered_json_object(
        callback_context,
        buffer_key=STATE_PLANNER_BUFFER,
        llm_response=llm_response,
    )
    if parsed is None:
        callback_context.state[STATE_TURN_ABORT_REASON] = "planner_parse_error"
        rendered = _render_parse_error_markdown("Planner", parse_error or "Failed to parse plan JSON", text)
        _set_turn_rendered_output(callback_context, key=STATE_PLANNER_RENDERED, text=rendered)
        return _replace_llm_response_text(llm_response, rendered)

    try:
        task_state = _initialize_task_state_from_plan(parsed, objective_text=_extract_user_turn_text(callback_context))
    except Exception as exc:  # noqa: BLE001
        callback_context.state[STATE_TURN_ABORT_REASON] = "planner_validation_error"
        rendered = _render_parse_error_markdown("Planner", str(exc), text)
        _set_turn_rendered_output(callback_context, key=STATE_PLANNER_RENDERED, text=rendered)
        return _replace_llm_response_text(llm_response, rendered)

    callback_context.state[STATE_WORKFLOW_TASK] = task_state
    rendered = _render_plan_markdown(task_state)
    _set_turn_rendered_output(callback_context, key=STATE_PLANNER_RENDERED, text=rendered)
    return _replace_llm_response_text(llm_response, rendered)


def _executor_before_model_callback(*, callback_context: CallbackContext, llm_request: LlmRequest) -> LlmResponse | None:
    if bool(callback_context.state.get(STATE_FINALIZE_REQUESTED, False)):
        return _make_text_response("")
    if str(callback_context.state.get(STATE_TURN_ABORT_REASON, "")).strip():
        return _make_text_response("")

    task_state = _get_task_state(callback_context)
    if not task_state:
        return _make_text_response("")

    current_step_id = str(task_state.get("current_step_id") or "").strip()
    if not current_step_id or str(task_state.get("plan_status", "")) == "completed":
        rendered = _render_all_steps_complete_message()
        _set_turn_rendered_output(callback_context, key=STATE_EXECUTOR_RENDERED, text=rendered)
        return _make_text_response(rendered)

    try:
        _, active_step = _find_step(task_state, current_step_id)
    except Exception as exc:  # noqa: BLE001
        callback_context.state[STATE_TURN_ABORT_REASON] = "executor_state_error"
        rendered = f"## Execution\n\nInvalid task state: {exc}"
        _set_turn_rendered_output(callback_context, key=STATE_EXECUTOR_RENDERED, text=rendered)
        return _make_text_response(rendered)

    existing_active_step_id = str(callback_context.state.get(STATE_EXECUTOR_ACTIVE_STEP_ID, "") or "")
    if existing_active_step_id != current_step_id:
        callback_context.state[STATE_EXECUTOR_ACTIVE_STEP_ID] = current_step_id
        callback_context.state[STATE_EXECUTOR_PREV_STEP_STATUS] = str(active_step.get("status", "pending"))
        active_step["status"] = "in_progress"
        task_state["steps"] = task_state.get("steps", [])
        callback_context.state[STATE_WORKFLOW_TASK] = task_state

    llm_request.config = llm_request.config or types.GenerateContentConfig()
    # Gemini tool/function calling is incompatible with forcing application/json response MIME.
    # Keep executor JSON output prompt-enforced instead, since this agent uses MCP tools.
    llm_request.config.response_mime_type = None
    llm_request.append_instructions(_executor_context_instructions(task_state, active_step))
    return None


def _executor_after_model_callback(*, callback_context: CallbackContext, llm_response: LlmResponse) -> LlmResponse | None:
    if _llm_response_has_function_call(llm_response):
        callback_context.state[STATE_EXECUTOR_BUFFER] = ""
        return None

    text = _llm_response_text(llm_response)
    if bool(getattr(llm_response, "partial", False)):
        _buffer_partial_text(callback_context, STATE_EXECUTOR_BUFFER, text)
        return _replace_llm_response_text(llm_response, "")

    if not text and not str(callback_context.state.get(STATE_EXECUTOR_BUFFER, "") or ""):
        return None

    parsed, parse_error = _consume_buffered_json_object(
        callback_context,
        buffer_key=STATE_EXECUTOR_BUFFER,
        llm_response=llm_response,
    )

    task_state = _get_task_state(callback_context)
    active_step_id = str(callback_context.state.get(STATE_EXECUTOR_ACTIVE_STEP_ID, "") or "")
    prev_status = str(callback_context.state.get(STATE_EXECUTOR_PREV_STEP_STATUS, "") or "pending")

    def _clear_executor_run_temps() -> None:
        callback_context.state[STATE_EXECUTOR_ACTIVE_STEP_ID] = ""
        callback_context.state[STATE_EXECUTOR_PREV_STEP_STATUS] = ""

    def _restore_step_status_on_failure() -> None:
        nonlocal task_state
        if not task_state or not active_step_id:
            return
        try:
            _, step = _find_step(task_state, active_step_id)
        except Exception:  # noqa: BLE001
            return
        step["status"] = prev_status or "pending"
        callback_context.state[STATE_WORKFLOW_TASK] = task_state

    if parsed is None or task_state is None:
        _restore_step_status_on_failure()
        _clear_executor_run_temps()
        callback_context.state[STATE_TURN_ABORT_REASON] = "executor_parse_error"
        rendered = _render_parse_error_markdown("Executor", parse_error or "Failed to parse executor JSON", text)
        _set_turn_rendered_output(callback_context, key=STATE_EXECUTOR_RENDERED, text=rendered)
        return _replace_llm_response_text(llm_response, rendered)

    try:
        applied_results = _apply_execution_batch_result_to_task_state(task_state, parsed)
    except Exception as exc:  # noqa: BLE001
        _restore_step_status_on_failure()
        _clear_executor_run_temps()
        callback_context.state[STATE_TURN_ABORT_REASON] = "executor_validation_error"
        rendered = _render_parse_error_markdown("Executor", str(exc), text)
        _set_turn_rendered_output(callback_context, key=STATE_EXECUTOR_RENDERED, text=rendered)
        return _replace_llm_response_text(llm_response, rendered)

    callback_context.state[STATE_WORKFLOW_TASK] = task_state
    callback_context.state[STATE_AUTO_SYNTH_REQUESTED] = str(task_state.get("plan_status", "")) == "completed"
    _clear_executor_run_temps()
    rendered = _render_executor_batch_progress_markdown(task_state, applied_results)
    _set_turn_rendered_output(callback_context, key=STATE_EXECUTOR_RENDERED, text=rendered)
    return _replace_llm_response_text(llm_response, rendered)


def _synth_before_model_callback(*, callback_context: CallbackContext, llm_request: LlmRequest) -> LlmResponse | None:
    wants_finalize = bool(callback_context.state.get(STATE_FINALIZE_REQUESTED, False))
    wants_auto_synth = bool(callback_context.state.get(STATE_AUTO_SYNTH_REQUESTED, False))
    if not wants_finalize and not wants_auto_synth:
        return _make_text_response(_compose_non_finalize_turn_output(callback_context))

    if str(callback_context.state.get(STATE_TURN_ABORT_REASON, "")).strip():
        return _make_text_response(_compose_non_finalize_turn_output(callback_context))

    task_state = _get_task_state(callback_context)
    if not task_state:
        return _make_text_response(_render_no_plan_to_finalize_message())

    callback_context.state[STATE_SYNTH_BUFFER] = ""
    llm_request.config = llm_request.config or types.GenerateContentConfig()
    llm_request.config.response_mime_type = None
    llm_request.append_instructions(_synth_context_instructions(task_state))
    return None


def _synth_after_model_callback(*, callback_context: CallbackContext, llm_response: LlmResponse) -> LlmResponse | None:
    if _llm_response_has_function_call(llm_response):
        return None

    text = _llm_response_text(llm_response)
    if bool(getattr(llm_response, "partial", False)):
        _buffer_partial_text(callback_context, STATE_SYNTH_BUFFER, text)
        return _replace_llm_response_text(llm_response, "")

    task_state = _get_task_state(callback_context)
    if task_state is None:
        return None

    buffered = str(callback_context.state.get(STATE_SYNTH_BUFFER, "") or "")
    callback_context.state[STATE_SYNTH_BUFFER] = ""
    final_markdown = _postprocess_synth_markdown(task_state, (buffered + text).strip())
    task_state["latest_synthesis"] = {
        "schema": "final_synthesis_text.v1",
        "coverage_status": _compute_coverage_status(task_state),
        "markdown": final_markdown,
    }
    callback_context.state[STATE_WORKFLOW_TASK] = task_state
    return _replace_llm_response_text(llm_response, final_markdown)


def _build_evidence_executor_instruction(tool_hints: list[str], *, prefer_bigquery: bool) -> str:
    tool_catalog = "\n".join(f"- {name}" for name in tool_hints[:80]) or "- No tools available."
    if prefer_bigquery:
        bq_policy = (
            "- BigQuery-first policy:\n"
            "  - For structured/tabular analysis, start with `list_bigquery_tables` and `run_bigquery_select_query`.\n"
            "  - Use non-BigQuery tools for enrichment, freshness gaps, or unavailable data."
        )
    else:
        bq_policy = "- BigQuery-first policy is disabled for this run."

    return (
        EVIDENCE_EXECUTOR_INSTRUCTION_TEMPLATE
        .replace("__TOOL_CATALOG__", tool_catalog)
        .replace("__BQ_POLICY__", bq_policy)
    )


def _build_planner_instruction(tool_hints: list[str], *, prefer_bigquery: bool) -> str:
    tool_catalog = "\n".join(f"- {name}" for name in tool_hints[:80]) or "- No tools available."
    if prefer_bigquery:
        bq_policy = (
            "- BigQuery-first policy:\n"
            "  - Prefer `list_bigquery_tables` and `run_bigquery_select_query` for structured/tabular subtasks.\n"
            "  - Use non-BigQuery tools for enrichment, freshness gaps, or unavailable data."
        )
    else:
        bq_policy = "- BigQuery-first policy is disabled for this run."

    return (
        PLANNER_INSTRUCTION_TEMPLATE
        .replace("__TOOL_CATALOG__", tool_catalog)
        .replace("__BQ_POLICY__", bq_policy)
    )


def create_mcp_toolset(tool_filter: list[str] | None = None) -> McpToolset | None:
    """Build an MCP toolset for the native evidence-executor agent."""
    if tool_filter is not None and len(tool_filter) == 0:
        return None

    server_params = StdioServerParameters(
        command="node",
        args=["server.js"],
        cwd=str(MCP_SERVER_DIR),
    )
    connection_params = StdioConnectionParams(
        server_params=server_params,
        timeout=90.0,
    )
    return McpToolset(
        connection_params=connection_params,
        tool_filter=tool_filter,
    )


def create_workflow_agent(
    *,
    tool_filter: list[str] | None = None,
    model: str | None = None,
    max_plan_iterations: int | None = None,
    prefer_bigquery: bool | None = None,
) -> tuple[SequentialAgent, McpToolset | None]:
    """Create an ADK-native workflow graph and return (root_agent, mcp_toolset)."""
    del max_plan_iterations

    runtime_model = str(model or DEFAULT_MODEL).strip() or DEFAULT_MODEL
    use_bigquery_priority = DEFAULT_PREFER_BIGQUERY if prefer_bigquery is None else bool(prefer_bigquery)

    mcp_toolset = create_mcp_toolset(tool_filter=tool_filter)
    executor_tools = [mcp_toolset] if mcp_toolset is not None else []
    base_tool_hints = _dedupe_str_list(tool_filter if tool_filter else KNOWN_MCP_TOOLS, limit=120)
    if use_bigquery_priority:
        base_hint_set = set(base_tool_hints)
        prioritized_hints = [name for name in BQ_PRIORITY_TOOLS if name in base_hint_set]
        prioritized_set = set(prioritized_hints)
        prioritized_hints.extend([name for name in base_tool_hints if name not in prioritized_set])
        executor_tool_hints = prioritized_hints
    else:
        executor_tool_hints = base_tool_hints

    planner = LlmAgent(
        name="planner",
        model=runtime_model,
        instruction=_build_planner_instruction(
            executor_tool_hints,
            prefer_bigquery=use_bigquery_priority,
        ),
        tools=[],
        disallow_transfer_to_parent=True,
        before_model_callback=_planner_before_model_callback,
        after_model_callback=_planner_after_model_callback,
    )
    evidence_executor = LlmAgent(
        name="evidence_executor",
        model=runtime_model,
        instruction=_build_evidence_executor_instruction(
            executor_tool_hints,
            prefer_bigquery=use_bigquery_priority,
        ),
        tools=executor_tools,
        before_model_callback=_executor_before_model_callback,
        after_model_callback=_executor_after_model_callback,
    )
    report_synthesizer = LlmAgent(
        name="report_synthesizer",
        model=runtime_model,
        instruction=SYNTHESIZER_INSTRUCTION,
        tools=[],
        before_model_callback=_synth_before_model_callback,
        after_model_callback=_synth_after_model_callback,
    )

    root = SequentialAgent(
        name="co_scientist_workflow",
        description="ADK-native biomedical workflow: planner, executor, synthesis.",
        sub_agents=[planner, evidence_executor, report_synthesizer],
    )
    return root, mcp_toolset


__all__ = [
    "create_mcp_toolset",
    "create_workflow_agent",
]
