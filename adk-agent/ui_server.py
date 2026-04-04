"""
Web UI for AI Co-Scientist (adapted to ADK-native workflow).

Run:
    python ui_server.py
"""
from __future__ import annotations

import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import contextlib
import copy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import re
import sys
import threading
import time
import traceback
import uuid

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from google.adk import Runner
from google.adk.sessions import InMemorySessionService
from google.genai.types import Content, Part
from pydantic import BaseModel, Field

from agent import validate_runtime_configuration
from state_store import SupportsWorkflowStateStore, create_state_store
from report_pdf import write_markdown_pdf
from co_scientist.analysis_workspace import (
    append_analysis_note_artifact,
    append_dataset_catalog_artifact,
    ensure_analysis_workspace,
    set_selected_dataset,
    task_analysis_snapshot,
)
from co_scientist.analysis_notebook import (
    build_analysis_notebook,
    notebook_runtime_status,
    notebook_json_for_api,
    serialize_notebook_ipynb,
)
from co_scientist.tool_registry import TOOL_SOURCE_NAMES
from co_scientist.workflow import (
    STATE_ACTIVE_TASK_CONTEXT,
    STATE_ANALYSIS_NOTEBOOK,
    STATE_ANALYSIS_WORKSPACE,
    STATE_EXECUTOR_ACTIVE_STEP_ID,
    STATE_EXECUTOR_LAST_ERROR,
    STATE_PRIOR_RESEARCH,
    STATE_WORKFLOW_TASK,
    _build_semantic_evidence_graph,
    _derive_step_data_sources,
    _describe_tool_call,
    STATE_PLAN_PENDING_APPROVAL,
    STATE_REACT_PARSE_RETRIES,
    _is_continue_execution_command,
    create_workflow_agent,
    _resolve_source_label,
)

load_dotenv()
logger = logging.getLogger(__name__)

RATE_LIMIT_QUERIES = int(os.environ.get("RATE_LIMIT_QUERIES", "20"))
RATE_LIMIT_WINDOW = int(os.environ.get("RATE_LIMIT_WINDOW", "3600"))
MAX_CONCURRENT_TURNS = int(os.environ.get("ADK_MAX_CONCURRENT_TURNS", "6"))
TASK_MODE_REPORT = "report"
TASK_MODE_ANALYSIS = "analysis"
VALID_TASK_MODES = {TASK_MODE_REPORT, TASK_MODE_ANALYSIS}
_global_turn_semaphore = threading.Semaphore(MAX_CONCURRENT_TURNS)
GA4_MEASUREMENT_ID = os.environ.get("GA4_MEASUREMENT_ID", "").strip()
_GA4_ID_PATTERN = re.compile(r"^G-[A-Z0-9]+$")
ANALYSIS_NOTEBOOK_MAX_RETRIES = 0
_ANALYSIS_NOTEBOOK_RETRYABLE_KINDS = frozenset()


def _project_venv_python() -> Path:
    return Path(__file__).resolve().parents[1] / ".venv" / "bin" / "python"


def _format_notebook_runtime_startup_line() -> str:
    status = notebook_runtime_status()
    return (
        "[ui] Notebook runtime: "
        f"python={status.get('python_executable') or sys.executable} "
        f"(v{status.get('python_version') or 'unknown'}), "
        f"pandas={'ok' if status.get('pandas', {}).get('available') else 'missing'}, "
        f"numpy={'ok' if status.get('numpy', {}).get('available') else 'missing'}, "
        f"matplotlib={'ok' if status.get('matplotlib', {}).get('available') else 'missing'}, "
        f"seaborn={'ok' if status.get('seaborn', {}).get('available') else 'missing'}"
    )


class RateLimiter:
    """In-memory sliding-window rate limiter keyed by IP address."""

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window = window_seconds
        self._hits: dict[str, list[float]] = defaultdict(list)

    def check(self, key: str) -> tuple[bool, int]:
        now = time.time()
        hits = self._hits[key]
        self._hits[key] = hits = [t for t in hits if now - t < self.window]
        if len(hits) >= self.max_requests:
            retry_after = int(self.window - (now - hits[0])) + 1
            return False, retry_after
        hits.append(now)
        return True, 0

    def remaining(self, key: str) -> int:
        now = time.time()
        hits = [t for t in self._hits.get(key, []) if now - t < self.window]
        return max(0, self.max_requests - len(hits))


query_limiter = RateLimiter(RATE_LIMIT_QUERIES, RATE_LIMIT_WINDOW)


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _enforce_rate_limit(request: Request) -> None:
    ip = _client_ip(request)
    allowed, retry_after = query_limiter.check(ip)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded. You can run {RATE_LIMIT_QUERIES} queries per hour. Try again in {retry_after}s.",
            headers={"Retry-After": str(retry_after)},
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _close_worker_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
    with contextlib.suppress(Exception):
        loop.run_until_complete(loop.shutdown_asyncgens())
    with contextlib.suppress(Exception):
        loop.run_until_complete(loop.shutdown_default_executor())
    asyncio.set_event_loop(None)
    loop.close()


def _compact_text(value: str, *, max_chars: int = 180) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _normalize_task_mode(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in VALID_TASK_MODES:
        return normalized
    return TASK_MODE_REPORT


def _derive_run_error_message(response_text: str, default: str) -> str:
    text = str(response_text or "").strip()
    if not text:
        return default
    cleaned = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
    cleaned = cleaned.replace("`", "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return _compact_text(cleaned, max_chars=260) or default


def _visible_event_text(parts) -> str:
    """Return only user-visible text from streamed model parts."""
    if not parts:
        return ""
    return "".join(
        str(getattr(part, "text", "") or "")
        for part in parts
        if isinstance(getattr(part, "text", None), str)
        and not bool(getattr(part, "thought", False))
    ).strip()


def _is_terminal_workflow_error_response(text: str) -> bool:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return False
    return (
        normalized.startswith("## execution error")
        or normalized.startswith("## rate limited")
        or "quota or rate limit exhausted" in normalized
        or "rate limits have been hit" in normalized
    )


def _fire_and_forget_threadsafe(coro: Any, loop: asyncio.AbstractEventLoop, *, label: str = "") -> None:
    future = asyncio.run_coroutine_threadsafe(coro, loop)

    def _log_failure(done_future) -> None:
        try:
            done_future.result()
        except Exception as exc:  # noqa: BLE001
            if label:
                logger.debug("%s failed: %s", label, exc)
            else:
                logger.debug("Threadsafe background task failed: %s", exc)

    future.add_done_callback(_log_failure)


_TRANSIENT_WORKFLOW_RESPONSE_PATTERNS = (
    re.compile(r"^_?\s*rate limit hit\s+[—-]\s+waited\s+\d+s,\s+retrying[.…_ ]*$", re.IGNORECASE),
    re.compile(r"^_?\s*rate limit hit(?:\s+from\s+.+?)?\s+[—-]\s+retry\s+\d+/\d+,\s+waited\s+\d+s[.…_ ]*$", re.IGNORECASE),
    re.compile(r"^_?\s*temporary model outage\s+from\s+.+?\s+[—-]\s+retry\s+\d+/\d+,\s+waited\s+\d+s[.…_ ]*$", re.IGNORECASE),
)

PERSISTED_SESSION_STATE_KEYS = (
    STATE_WORKFLOW_TASK,
    STATE_ANALYSIS_WORKSPACE,
    STATE_ANALYSIS_NOTEBOOK,
    STATE_PRIOR_RESEARCH,
    STATE_PLAN_PENDING_APPROVAL,
)


def _is_transient_workflow_response(text: str) -> bool:
    normalized = str(text or "").strip()
    if not normalized:
        return False
    return any(pattern.match(normalized) for pattern in _TRANSIENT_WORKFLOW_RESPONSE_PATTERNS)


def _parse_step_event_text(text: str) -> dict:
    """Extract structured info from step_executor rendered markdown."""
    info: dict = {}
    normalized = str(text or "").strip()
    if not normalized:
        return info

    # If the payload contains multiple rendered steps, use the latest one.
    step_matches = list(re.finditer(r"^###?\s+S\d+\s*[·\-]\s*`?\w+`?", normalized, flags=re.MULTILINE))
    if step_matches:
        normalized = normalized[step_matches[-1].start():].strip()

    # Step ID and status: "### S1 · `completed`"
    m = re.search(r"###?\s+(S\d+)\s*[·\-]\s*`?(\w+)`?", normalized)
    if m:
        info["step_id"] = m.group(1)
        info["status"] = m.group(2)
    # Goal: "**Goal:** ..."
    m = re.search(r"\*\*Goal:\*\*\s*(.+?)(?:\n|$)", normalized)
    if m:
        info["goal"] = m.group(1).strip()
    # Findings (supports "Key Findings", "Detailed Findings", "Findings")
    m = re.search(r"\*\*(?:Key |Detailed )?Findings\*\*\s*\n+([\s\S]*?)(?=\n\*\*|\n_Progress|\n---|\Z)", normalized)
    if m:
        info["findings"] = re.sub(r"\s+", " ", m.group(1)).strip()[:300]
    # Tools used: "**Tools used:** `tool1`, `tool2`"
    m = re.search(r"\*\*Tools used:\*\*\s*(.+?)(?:\n|$)", normalized)
    if m:
        info["tools"] = re.findall(r"`([^`]+)`", m.group(1))
    # Evidence IDs
    evidence = re.findall(r"`((?:PMID|DOI|NCT|PMC|UniProt|PubChem|PDB|ChEMBL|Reactome)[:\s][^`]+|(?:GCST|CHEMBL|rs)\S[^`]*)`", normalized)
    if evidence:
        info["evidence"] = evidence[:10]
    # Progress: "_Progress: 2/5 steps complete..."
    m = re.search(r"_Progress:\s*(.+?)_", normalized)
    if m:
        info["progress"] = m.group(1).strip()
    # ReAct trace block
    m = re.search(r"\*\*(?:ReAct Trace|Tool Trace)\*\*\s*\n+([\s\S]*?)(?=\n\*\*|\n_Progress|\n---|\Z)", normalized)
    if m:
        block = m.group(1)
        cleaned_lines: list[str] = []
        phase_map: dict[str, str] = {}
        phase_pattern = re.compile(r"\*\*(Reason|Act|Observe|Conclude):\*\*\s*(.+)", flags=re.IGNORECASE)
        for raw_line in block.splitlines():
            line = re.sub(r"^\s*>\s?", "", raw_line).strip()
            if not line:
                continue
            cleaned_lines.append(line)
            phase_match = phase_pattern.match(line)
            if phase_match:
                phase_map[phase_match.group(1).lower()] = phase_match.group(2).strip()
        if cleaned_lines:
            info["react_trace"] = "\n".join(cleaned_lines)
        if phase_map:
            info["react_phases"] = phase_map
    return info


def _build_step_completed_event_metrics(text: str) -> dict | None:
    """Return structured metrics only for actual rendered step-completion blocks."""
    step_info = _parse_step_event_text(text)
    step_id = str(step_info.get("step_id", "") or "").strip()
    if not step_id:
        return None
    return {
        "step_id": step_id,
        "step_status": str(step_info.get("status", "completed") or "completed").strip(),
        "goal": str(step_info.get("goal", "") or "").strip(),
        "findings": str(step_info.get("findings", "") or "").strip(),
        "tools": list(step_info.get("tools", []) or []),
        "evidence": list(step_info.get("evidence", []) or []),
        "progress": str(step_info.get("progress", "") or "").strip(),
        "react_trace": str(step_info.get("react_trace", "") or "").strip(),
        "react_phases": step_info.get("react_phases", {}) if isinstance(step_info.get("react_phases", {}), dict) else {},
        "rendered_step_markdown": text,
    }


def _extract_executor_retry_metrics(session_state: dict | None) -> dict | None:
    """Extract executor parse-retry diagnostics from persisted session state."""
    if not isinstance(session_state, dict):
        return None
    step_id = str(session_state.get(STATE_EXECUTOR_ACTIVE_STEP_ID, "") or "").strip()
    retry_count = int(session_state.get(STATE_REACT_PARSE_RETRIES, 0) or 0)
    error = str(session_state.get(STATE_EXECUTOR_LAST_ERROR, "") or "").strip()
    if not step_id and retry_count <= 0 and not error:
        return None
    return {
        "step_id": step_id,
        "retry_count": retry_count,
        "error": error,
    }


def _extract_tool_error_metrics(function_response) -> dict | None:
    """Extract a user-visible tool error payload from a function response."""
    if not function_response:
        return None
    tool_name = str(getattr(function_response, "name", "") or "").strip()
    response = getattr(function_response, "response", None) or {}
    if not isinstance(response, dict) or not bool(response.get("error")):
        return None
    message = str(response.get("message", "") or "").strip()
    suggestion = str(response.get("suggestion", "") or "").strip()
    error_type = str(response.get("error_type", "") or "").strip()
    if not tool_name and not message:
        return None
    return {
        "tool": tool_name,
        "error_type": error_type,
        "message": message,
        "suggestion": suggestion,
    }


_NOTEBOOK_SYNTH_TOOL_NAMES = frozenset(
    {
        "store_dataset_catalog",
        "store_notebook_code_cell",
        "store_dataset_visualizations",
        "store_dataset_metadata_profile",
        "append_analysis_note",
    }
)


def _notebook_synthesizer_tool_outcome(function_response, *, author: str) -> dict | None:
    """Research-log line for analysis notebook tool success or ok=false (plotting / cells)."""
    if str(author or "").strip() != "analysis_notebook_synthesizer":
        return None
    if not function_response:
        return None
    tool_name = str(getattr(function_response, "name", "") or "").strip()
    if tool_name not in _NOTEBOOK_SYNTH_TOOL_NAMES:
        return None
    response = getattr(function_response, "response", None)
    if response is None:
        return None
    if not isinstance(response, dict):
        return {
            "event_type": "notebook.tool.completed",
            "status": "done",
            "human_line": _compact_text(f"{tool_name} returned non-dict response.", max_chars=220),
            "metrics": {"tool": tool_name},
        }
    if bool(response.get("error")):
        return None
    message = _compact_text(str(response.get("message") or ""), max_chars=200)
    if response.get("ok") is False:
        return {
            "event_type": "tool.failed",
            "status": "error",
            "human_line": _compact_text(
                f"Notebook: {tool_name} failed — {message or 'ok=false'}",
                max_chars=220,
            ),
            "metrics": {"tool": tool_name, "message": message},
        }
    line = message
    if tool_name == "store_dataset_catalog":
        cards = response.get("summary_cards") if isinstance(response.get("summary_cards"), dict) else {}
        line = (
            f"{message or 'Notebook: stored dataset comparison.'} "
            f"({cards.get('dataset_count', '?')} datasets × {cards.get('dimension_count', '?')} axes)"
        )
    elif tool_name == "store_dataset_visualizations":
        line = message or "Notebook: stored dataset visualization bundle."
    elif tool_name == "store_notebook_code_cell":
        line = message or "Notebook: stored agent-authored notebook code cell."
    elif tool_name == "store_dataset_metadata_profile":
        line = message or "Notebook: stored metadata profile cell."
    elif tool_name == "append_analysis_note":
        line = message or "Notebook: appended analysis note."
    return {
        "event_type": "notebook.tool.completed",
        "status": "done",
        "human_line": _compact_text(line or f"{tool_name} ok.", max_chars=220),
        "metrics": {"tool": tool_name, "ok": True},
    }




def _generate_chat_title(query: str) -> str:
    words = re.sub(r"\s+", " ", str(query or "")).strip().split()
    if len(words) <= 8:
        return " ".join(words) or "Research"
    return " ".join(words[:8]).rstrip(".,;:!?")


def _extract_persistable_session_state(session_state: dict | None) -> dict:
    persisted: dict[str, object] = {}
    if not isinstance(session_state, dict):
        return persisted
    for key in PERSISTED_SESSION_STATE_KEYS:
        if key in session_state and session_state[key] is not None:
            persisted[key] = copy.deepcopy(session_state[key])
    return persisted


def _merge_analysis_runtime_context(
    task_state: dict | None,
    *,
    conversation_id: str = "",
    task_id: str = "",
    user_prompt: str = "",
    mode: str = TASK_MODE_ANALYSIS,
) -> dict[str, str]:
    existing = dict(task_state.get("analysis_runtime_context") or {}) if isinstance(task_state, dict) else {}
    resolved = {
        "conversation_id": str(
            conversation_id or existing.get("conversation_id") or ""
        ).strip(),
        "task_id": str(task_id or existing.get("task_id") or "").strip(),
        "user_prompt": str(
            user_prompt
            or existing.get("user_prompt")
            or (task_state.get("objective") if isinstance(task_state, dict) else "")
            or ""
        ).strip(),
        "mode": _normalize_task_mode(mode or existing.get("mode") or TASK_MODE_ANALYSIS),
    }
    if isinstance(task_state, dict) and any(resolved.get(key) for key in ("conversation_id", "task_id", "user_prompt")):
        task_state["analysis_runtime_context"] = resolved
    return resolved


def _rehydrate_analysis_runtime_state(
    session_state: dict | None,
    *,
    conversation_id: str,
    task_id: str = "",
    mode: str = TASK_MODE_ANALYSIS,
) -> dict:
    state = dict(session_state or {})
    task_state = state.get(STATE_WORKFLOW_TASK)
    if not isinstance(task_state, dict):
        task_state = {}
    resolved = _merge_analysis_runtime_context(
        task_state,
        conversation_id=conversation_id,
        task_id=task_id,
        mode=mode,
    )
    if task_state:
        state[STATE_WORKFLOW_TASK] = task_state
    if resolved.get("task_id"):
        state[STATE_ACTIVE_TASK_CONTEXT] = dict(resolved)
    return state


# ---------------------------------------------------------------------------
# Conversation session: ADK runner + session per conversation
# ---------------------------------------------------------------------------

@dataclass
class ConversationSession:
    runner: Runner
    session_id: str
    app_name: str
    mcp_tools: object | None
    mode: str = TASK_MODE_REPORT


# ---------------------------------------------------------------------------
# RunRecord: tracks background execution progress
# ---------------------------------------------------------------------------

@dataclass
class RunRecord:
    run_id: str
    kind: str
    status: str = "queued"
    task_id: str | None = None
    query: str = ""
    title: str = ""
    logs: list[dict] = field(default_factory=list)
    progress_events: list[dict] = field(default_factory=list)
    progress_summaries: list[dict] = field(default_factory=list)
    final_report: str | None = None
    follow_up_suggestions: list[str] = field(default_factory=list)
    clarification: str | None = None
    error: str | None = None
    created_at: str = field(default_factory=_utc_now)
    updated_at: str = field(default_factory=_utc_now)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "kind": self.kind,
            "status": self.status,
            "task_id": self.task_id,
            "query": self.query,
            "title": self.title,
            "logs": list(self.logs),
            "progress_events": list(self.progress_events),
            "progress_summaries": list(self.progress_summaries),
            "final_report": self.final_report,
            "follow_up_suggestions": list(self.follow_up_suggestions),
            "clarification": self.clarification,
            "error": self.error,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class QueryRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=5000)
    conversation_id: str | None = Field(default=None, max_length=128)
    parent_task_id: str | None = Field(default=None, max_length=128)
    mode: str = Field(default=TASK_MODE_REPORT, max_length=32)


class FeedbackRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)


class ReviseRequest(BaseModel):
    scope: str = Field(..., min_length=1, max_length=5000)


class StartRequest(BaseModel):
    plan_version_id: str | None = Field(default=None, max_length=128)


class RollbackRequest(BaseModel):
    token: str = Field(..., min_length=1, max_length=256)


class AnalysisSelectionRequest(BaseModel):
    dataset_id: str = Field(..., min_length=1, max_length=256)


# ---------------------------------------------------------------------------
# Workflow task helpers
# ---------------------------------------------------------------------------

def _make_task(
    task_id: str,
    objective: str,
    conversation_id: str,
    *,
    title: str = "",
    user_query: str = "",
    parent_task_id: str | None = None,
    mode: str = TASK_MODE_REPORT,
) -> dict:
    now = _utc_now()
    return {
        "task_id": task_id,
        "title": title or _generate_chat_title(objective),
        "conversation_id": conversation_id,
        "parent_task_id": parent_task_id,
        "mode": _normalize_task_mode(mode),
        "objective": objective,
        "user_query": user_query or objective,
        "status": "in_progress",
        "awaiting_hitl": False,
        "current_step_index": 0,
        "steps": [],
        "hitl_history": [],
        "follow_up_suggestions": [],
        "branch_label": "",
        "created_at": now,
        "updated_at": now,
        "progress_events": [],
        "progress_summaries": [],
        "dataset_visualizations": None,
        "analysis_operation_id": "",
        "analysis_cell_ids": [],
        "analysis_workspace_revision": 0,
        "selected_dataset_id_snapshot": "",
    }


def _steps_from_workflow_state(wf_state: dict | None) -> list[dict]:
    if not wf_state:
        return []
    def _source_label(tool_name: str) -> str:
        return _resolve_source_label(tool_name or "")

    return [
        {
            "title": step.get("goal", f"Step {step.get('id', '?')}"),
            "instruction": (
                f"Potential source: {_source_label(step.get('tool_hint', ''))}. "
                f"Done when: {str(step.get('completion_condition', '')).strip()}"
            ),
            "status": step.get("status", "pending"),
            "id": step.get("id", ""),
            "tool_hint": str(step.get("tool_hint", "")).strip(),
            "source": _source_label(step.get("tool_hint", "")),
            "completion_condition": str(step.get("completion_condition", "")).strip(),
            "result_summary": step.get("result_summary", ""),
            "tool_reasoning": step.get("tool_reasoning", ""),
            "evidence_refs": step.get("evidence_ids", []),
            "tool_trace": [
                {"tool": t} for t in step.get("tools_called", [])
            ],
            "output": step.get("result_summary", ""),
        }
        for step in wf_state.get("steps", [])
    ]


def _dataset_visualizations_from_workflow_state(wf_state: dict | None) -> dict | None:
    if not isinstance(wf_state, dict):
        return None
    bundle = wf_state.get("latest_dataset_visualizations")
    if not isinstance(bundle, dict):
        return None
    rows = list(bundle.get("rows", []) or [])
    if not rows:
        return None
    return copy.deepcopy(bundle)


def _sync_task_dataset_visualizations(task: dict, wf_state: dict | None) -> None:
    bundle = _dataset_visualizations_from_workflow_state(wf_state)
    if bundle is None:
        task["dataset_visualizations"] = None
        return
    task["dataset_visualizations"] = bundle


def _analysis_workspace_from_session_state(session_state: dict | None) -> dict | None:
    if not isinstance(session_state, dict):
        return None
    workspace = session_state.get(STATE_ANALYSIS_WORKSPACE)
    if not isinstance(workspace, dict):
        return None
    return workspace


def _analysis_notebook_from_session_state(session_state: dict | None) -> dict | None:
    if not isinstance(session_state, dict):
        return None
    notebook = session_state.get(STATE_ANALYSIS_NOTEBOOK)
    if not isinstance(notebook, dict):
        return None
    return notebook


def _recover_analysis_workspace_from_tasks(
    tasks: list[dict],
    *,
    workspace: dict | None,
    conversation_id: str,
) -> tuple[dict | None, bool]:
    if not tasks:
        return workspace, False
    recovered = ensure_analysis_workspace(
        workspace,
        conversation_id=conversation_id,
    )
    if list(recovered.get("cells", []) or []):
        return recovered, False

    changed = False
    ordered_tasks = sorted(
        tasks,
        key=lambda task: (
            str(task.get("created_at", "") or ""),
            str(task.get("task_id", "") or ""),
        ),
    )
    for task in ordered_tasks:
        if _normalize_task_mode(task.get("mode")) != TASK_MODE_ANALYSIS:
            continue
        task_id = str(task.get("task_id", "") or "").strip()
        if not task_id:
            continue
        user_prompt = str(task.get("user_query", "") or task.get("objective", "") or "").strip()
        bundle = task.get("dataset_visualizations")
        if isinstance(bundle, dict) and list(bundle.get("rows", []) or []):
            try:
                append_dataset_catalog_artifact(
                    recovered,
                    task_id=task_id,
                    user_prompt=user_prompt,
                    objective=str(bundle.get("objective") or task.get("objective") or "").strip(),
                    summary=str(bundle.get("summary") or task.get("objective") or "").strip(),
                    dimensions=list(bundle.get("dimensions", []) or []),
                    datasets=list(bundle.get("rows", []) or []),
                    notes=[
                        str(note).strip()
                        for note in list(bundle.get("notes", []) or [])
                        if str(note).strip()
                    ],
                )
                changed = True
            except ValueError as exc:
                logger.warning(
                    "Failed to recover dataset comparison notebook cell for task %s: %s",
                    task_id,
                    exc,
                )
        report_markdown = str(task.get("report_markdown", "") or "").strip()
        if report_markdown:
            try:
                append_analysis_note_artifact(
                    recovered,
                    task_id=task_id,
                    user_prompt=user_prompt,
                    markdown=report_markdown,
                    title=str(task.get("title", "") or task.get("objective", "") or "Analysis note").strip(),
                    related_dataset_ids=[],
                )
                changed = True
            except ValueError as exc:
                logger.warning(
                    "Failed to recover analysis note notebook cell for task %s: %s",
                    task_id,
                    exc,
                )
    return recovered, changed


def _sync_task_analysis_workspace_fields(task: dict, workspace: dict | None) -> None:
    if not isinstance(workspace, dict):
        task["analysis_operation_id"] = ""
        task["analysis_cell_ids"] = []
        task["analysis_workspace_revision"] = 0
        task["selected_dataset_id_snapshot"] = ""
        return
    snapshot = task_analysis_snapshot(
        workspace,
        task_id=str(task.get("task_id", "") or "").strip(),
    )
    task["analysis_operation_id"] = snapshot["analysis_operation_id"]
    task["analysis_cell_ids"] = list(snapshot["analysis_cell_ids"])
    task["analysis_workspace_revision"] = int(snapshot["analysis_workspace_revision"] or 0)
    task["selected_dataset_id_snapshot"] = str(snapshot["selected_dataset_id_snapshot"] or "")


def _analysis_workspace_has_task_cell_type(
    workspace: dict | None,
    *,
    task_id: str,
    cell_type: str,
) -> bool:
    if not isinstance(workspace, dict):
        return False
    normalized_task_id = str(task_id or "").strip()
    normalized_cell_type = str(cell_type or "").strip()
    if not normalized_task_id or not normalized_cell_type:
        return False
    return any(
        str(cell.get("task_id", "") or "").strip() == normalized_task_id
        and str(cell.get("type", "") or "").strip() == normalized_cell_type
        for cell in list(workspace.get("cells", []) or [])
        if isinstance(cell, dict)
    )


def _analysis_notebook_retry_needed(
    *,
    wf_state: dict | None,
    workspace: dict | None,
    task_id: str,
) -> bool:
    del wf_state, workspace, task_id
    return False


def _normalize_steps_for_ui(steps: list[dict] | None) -> list[dict]:
    """Ensure plan steps are user-facing (database names, not tool ids)."""
    normalized: list[dict] = []
    for raw_step in (steps or []):
        step = dict(raw_step or {})
        tool_hint = str(step.get("tool_hint", "")).strip()
        source = str(step.get("source", "")).strip()
        completion = str(step.get("completion_condition", "")).strip()
        instruction = str(step.get("instruction", "")).strip()

        if not source and tool_hint:
            source = _resolve_source_label(tool_hint)

        if not source and instruction:
            tool_match = re.search(r"Tool:\s*([^.\n]+)", instruction)
            if tool_match:
                inferred_tool = tool_match.group(1).strip()
                source = _resolve_source_label(inferred_tool)
                tool_hint = tool_hint or inferred_tool

        if not completion and instruction:
            done_match = re.search(r"Done when:\s*(.+)$", instruction)
            if done_match:
                completion = done_match.group(1).strip()

        if source and completion:
            step["instruction"] = f"Potential source: {source}. Done when: {completion}"
        elif source:
            step["instruction"] = f"Potential source: {source}."
        elif completion:
            step["instruction"] = f"Done when: {completion}"

        if tool_hint:
            step["tool_hint"] = tool_hint
        if source:
            step["source"] = source
        if completion:
            step["completion_condition"] = completion
        normalized.append(step)
    return normalized


def _task_summary(task: dict) -> dict:
    return {
        "task_id": task["task_id"],
        "title": task.get("title", ""),
        "conversation_id": task.get("conversation_id", ""),
        "parent_task_id": task.get("parent_task_id"),
        "objective": task.get("objective", ""),
        "user_query": task.get("user_query", task.get("objective", "")),
        "status": task.get("status", ""),
        "awaiting_hitl": bool(task.get("awaiting_hitl")),
        "current_step_index": task.get("current_step_index", 0),
        "step_count": len(task.get("steps", [])),
        "created_at": task.get("created_at", ""),
        "updated_at": task.get("updated_at", ""),
        "analysis_operation_id": task.get("analysis_operation_id", ""),
        "analysis_cell_ids": list(task.get("analysis_cell_ids", []) or []),
        "analysis_workspace_revision": int(task.get("analysis_workspace_revision", 0) or 0),
        "selected_dataset_id_snapshot": task.get("selected_dataset_id_snapshot", ""),
    }


def _task_detail(task: dict) -> dict:
    mode = _normalize_task_mode(task.get("mode"))
    dataset_visualizations = task.get("dataset_visualizations")
    has_dataset_visualizations = bool(
        isinstance(dataset_visualizations, dict)
        and list((dataset_visualizations.get("rows", []) or []))
    )
    return {
        **task,
        "mode": mode,
        "has_dataset_visualizations": has_dataset_visualizations,
        "has_analysis_cells": bool(list(task.get("analysis_cell_ids", []) or [])),
        "status_text": f"Status: {task.get('status', 'unknown')}",
        "quality_snapshot": {
            "passed": task.get("status") == "completed",
            "unresolved_gaps": [],
            "tool_call_count": 0,
            "evidence_count": 0,
            "quality_confidence": "",
            "quality_score": 0.0,
        },
        "planner_mode": "",
        "phase_state": {},
        "checkpoint_payload": {},
        "quality_confidence": "",
        "researcher_candidates": [],
        "event_log": [],
    }


def _iteration_from_task(task: dict, idx: int = 1) -> dict:
    steps = _normalize_steps_for_ui(task.get("steps", []))
    mode = _normalize_task_mode(task.get("mode"))
    has_dataset_visualizations = bool(
        isinstance(task.get("dataset_visualizations"), dict)
        and list(((task.get("dataset_visualizations") or {}).get("rows", []) or []))
    )
    active_plan = {
        "version_id": f"plan_{task['task_id']}",
        "steps": steps,
    } if steps else None

    is_direct = bool(task.get("is_direct_response"))
    report_md = task.get("report_markdown", "") if not is_direct else ""
    direct_response_text = task.get("direct_response_text", "") if is_direct else ""

    return {
        "iteration_index": idx,
        "task": _task_detail(task),
        "task_summary": _task_summary(task),
        "analysis": {
            "operation_id": task.get("analysis_operation_id", ""),
            "cell_ids": list(task.get("analysis_cell_ids", []) or []),
            "workspace_revision": int(task.get("analysis_workspace_revision", 0) or 0),
            "selected_dataset_id_snapshot": task.get("selected_dataset_id_snapshot", ""),
            "has_cells": bool(list(task.get("analysis_cell_ids", []) or [])),
        },
        "active_plan_version": active_plan,
        "latest_plan_delta": None,
        "is_direct_response": is_direct,
        "direct_response_text": direct_response_text,
        "research_log": {
            "task_id": task["task_id"],
            "events": task.get("progress_events", []),
            "summaries": task.get("progress_summaries", []),
            "stats": {},
            "started_at": task.get("created_at", ""),
            "ended_at": task.get("updated_at", "") if task.get("status") in ("completed", "failed") else None,
        },
        "report": {
            "report_markdown_path": None,
            "report_markdown": report_md,
            "report_pdf_path": None,
            "has_report": bool(report_md and str(report_md).strip()),
            "mode": mode,
            "has_dataset_visualizations": has_dataset_visualizations,
        },
        "follow_up_suggestions": task.get("follow_up_suggestions", []),
        "branch_label": task.get("branch_label", ""),
        "parent_task_id": task.get("parent_task_id"),
    }


# ---------------------------------------------------------------------------
# UiRuntime: main runtime managing sessions and execution
# ---------------------------------------------------------------------------

class UiRuntime:
    def __init__(self, state_store_path: Path) -> None:
        self.store: SupportsWorkflowStateStore = create_state_store(state_store_path)
        self.ready = False
        self.ready_error: str | None = None
        self.user_id = "researcher"
        self.session_service: InMemorySessionService | None = None
        self.conv_sessions: dict[str, ConversationSession] = {}
        self.conv_thread_locks: dict[str, threading.Lock] = {}
        self.runs_lock = asyncio.Lock()
        self.runs: dict[str, RunRecord] = {}
        self.background_tasks: set[asyncio.Task] = set()
        self._thread_pool = ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TURNS + 4, thread_name_prefix="wf")

    async def startup(self) -> None:
        is_valid, error_message = validate_runtime_configuration()
        if not is_valid:
            self.ready_error = error_message
            return
        self.session_service = InMemorySessionService()
        interrupted = self.store.mark_incomplete_runs_failed(
            "Run interrupted because the server restarted before completion."
        )
        if interrupted:
            logger.warning("Marked %d incomplete runs as failed during startup.", interrupted)
        self.ready = True
        self.ready_error = None
        logger.info(_format_notebook_runtime_startup_line())
        expected_venv_python = _project_venv_python()
        if expected_venv_python.exists():
            try:
                if Path(sys.executable).resolve() != expected_venv_python.resolve():
                    logger.warning(
                        "[ui] Server is running outside the project virtualenv. current=%s expected=%s",
                        sys.executable,
                        expected_venv_python,
                    )
            except Exception:
                logger.debug("Failed to compare current interpreter with project virtualenv.", exc_info=True)

    async def shutdown(self) -> None:
        pending = list(self.background_tasks)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for cs in self.conv_sessions.values():
            try:
                await cs.runner.close()
            except Exception:
                logger.debug("Failed to close ADK runner during shutdown.", exc_info=True)
            if cs.mcp_tools is not None:
                try:
                    await cs.mcp_tools.close()
                except Exception:
                    logger.debug("Failed to close MCP tools during shutdown.", exc_info=True)
        if self.conv_sessions:
            # Give async HTTP client cleanup tasks a chance to finish before
            # the process tears down the event loop.
            await asyncio.sleep(0.1)
        self._thread_pool.shutdown(wait=True)

    def _get_conv_thread_lock(self, conversation_id: str) -> threading.Lock:
        if conversation_id not in self.conv_thread_locks:
            self.conv_thread_locks[conversation_id] = threading.Lock()
        return self.conv_thread_locks[conversation_id]

    async def _get_or_create_session(
        self,
        conversation_id: str,
        *,
        mode: str = TASK_MODE_REPORT,
    ) -> ConversationSession:
        normalized_mode = _normalize_task_mode(mode)
        if conversation_id in self.conv_sessions:
            if normalized_mode == TASK_MODE_ANALYSIS:
                await self._update_session_state(
                    conversation_id,
                    lambda state: {
                        **(state or {}),
                        STATE_ANALYSIS_WORKSPACE: ensure_analysis_workspace(
                            _analysis_workspace_from_session_state(state),
                            conversation_id=conversation_id,
                        ),
                    },
                )
            return self.conv_sessions[conversation_id]
        if not self.session_service:
            raise RuntimeError("Session service is not initialized.")
        workflow_agent, mcp_tools = create_workflow_agent(
            require_plan_approval=True,
            workflow_mode=normalized_mode,
        )
        app_name = f"co_scientist_ui_{conversation_id}"
        runner = Runner(
            agent=workflow_agent,
            app_name=app_name,
            session_service=self.session_service,
        )
        restored_session = self.store.get_workflow_session(conversation_id)
        restored_state = None
        restored_task_id = ""
        if isinstance(restored_session, dict):
            restored_task_id = str(restored_session.get("task_id", "") or "").strip()
            payload = restored_session.get("state")
            if isinstance(payload, dict):
                restored_state = _extract_persistable_session_state(payload) or None
        if normalized_mode == TASK_MODE_ANALYSIS:
            restored_state = _rehydrate_analysis_runtime_state(
                restored_state,
                conversation_id=conversation_id,
                task_id=restored_task_id,
                mode=normalized_mode,
            )
            restored_state[STATE_ANALYSIS_WORKSPACE] = ensure_analysis_workspace(
                restored_state.get(STATE_ANALYSIS_WORKSPACE) if isinstance(restored_state, dict) else None,
                conversation_id=conversation_id,
            )
        session = await self.session_service.create_session(
            app_name=app_name,
            user_id=self.user_id,
            state=restored_state,
        )
        cs = ConversationSession(
            runner=runner,
            session_id=session.id,
            app_name=app_name,
            mcp_tools=mcp_tools,
            mode=normalized_mode,
        )
        self.conv_sessions[conversation_id] = cs
        return cs

    async def _get_session(self, conversation_id: str):
        cs = self.conv_sessions.get(conversation_id)
        if not cs or not self.session_service:
            return None
        return await self.session_service.get_session(
            app_name=cs.app_name,
            user_id=self.user_id,
            session_id=cs.session_id,
        )

    def _get_storage_session(self, conversation_id: str):
        cs = self.conv_sessions.get(conversation_id)
        if not cs or not isinstance(self.session_service, InMemorySessionService):
            return None
        return (
            self.session_service.sessions
            .get(cs.app_name, {})
            .get(self.user_id, {})
            .get(cs.session_id)
        )

    async def _update_session_state(
        self,
        conversation_id: str,
        updater,
    ) -> dict | None:
        session = self._get_storage_session(conversation_id)
        if session is None:
            session = await self._get_session(conversation_id)
        if not session:
            return None
        state = session.state if isinstance(session.state, dict) else {}
        updated = updater(state)
        if isinstance(updated, dict):
            if isinstance(session.state, dict):
                if updated is not state:
                    session.state.clear()
                    session.state.update(updated)
                return session.state
            session.state = updated
            return updated
        return session.state if isinstance(session.state, dict) else state

    async def _read_session_state(self, conversation_id: str) -> dict | None:
        session = await self._get_session(conversation_id)
        if not session:
            return None
        return session.state

    async def _read_workflow_state(self, conversation_id: str) -> dict | None:
        session_state = await self._read_session_state(conversation_id)
        if not session_state:
            return None
        return session_state.get(STATE_WORKFLOW_TASK)

    async def _read_analysis_workspace(self, conversation_id: str) -> dict | None:
        session_state = await self._read_session_state(conversation_id)
        if not session_state:
            return None
        workspace = _analysis_workspace_from_session_state(session_state)
        if workspace is None:
            return None
        return workspace

    async def _read_analysis_notebook(self, conversation_id: str) -> dict | None:
        session_state = await self._read_session_state(conversation_id)
        if not session_state:
            return None
        notebook = _analysis_notebook_from_session_state(session_state)
        if notebook is None:
            return None
        return notebook

    async def _retry_analysis_notebook_stage_if_needed(
        self,
        *,
        conversation_id: str,
        task_id: str,
        run_id: str,
        task: dict,
        response_text: str,
        responding_author: str,
        wf_state: dict | None,
    ) -> tuple[str, str, dict | None]:
        if _normalize_task_mode(task.get("mode")) != TASK_MODE_ANALYSIS:
            return response_text, responding_author, wf_state
        if ANALYSIS_NOTEBOOK_MAX_RETRIES <= 0:
            return response_text, responding_author, wf_state

        workspace = await self._read_analysis_workspace(conversation_id)
        if not _analysis_notebook_retry_needed(
            wf_state=wf_state,
            workspace=workspace,
            task_id=task_id,
        ):
            return response_text, responding_author, wf_state

        retry_prompt = "finalize"
        original_prompt = str(task.get("user_query", "") or task.get("objective", "") or "").strip()

        for attempt in range(1, ANALYSIS_NOTEBOOK_MAX_RETRIES + 1):
            await self._append_progress_event(
                run_id,
                phase="synthesize",
                event_type="notebook.retry",
                status="progress",
                human_line=(
                    f"Retrying notebook synthesis from stored analysis state "
                    f"(attempt {attempt}/{ANALYSIS_NOTEBOOK_MAX_RETRIES})."
                ),
                task_id=task_id,
                metrics={"attempt": attempt, "max_attempts": ANALYSIS_NOTEBOOK_MAX_RETRIES},
            )
            await self._set_active_task_context(
                conversation_id,
                task_id=task_id,
                user_prompt=original_prompt,
                mode=TASK_MODE_ANALYSIS,
            )
            response_text, responding_author = await self._run_workflow_turn_filtered(
                conversation_id,
                retry_prompt,
                run_id=run_id,
            )
            wf_state = await self._read_workflow_state(conversation_id)
            workspace = await self._read_analysis_workspace(conversation_id)
            if not _analysis_notebook_retry_needed(
                wf_state=wf_state,
                workspace=workspace,
                task_id=task_id,
            ):
                await self._append_progress_event(
                    run_id,
                    phase="synthesize",
                    event_type="notebook.retry.completed",
                    status="done",
                    human_line="Notebook synthesis retry completed using stored analysis state.",
                    task_id=task_id,
                    metrics={"attempt": attempt},
                )
                break

        return response_text, responding_author, wf_state

    async def _build_analysis_notebook_payload(
        self,
        *,
        conversation_id: str,
        workspace: dict | None,
        session_state: dict | None = None,
    ) -> dict | None:
        if not isinstance(workspace, dict):
            return None
        wf_state = None
        if isinstance(session_state, dict):
            wf_state = session_state.get(STATE_WORKFLOW_TASK)
        diagnostics = []
        if isinstance(wf_state, dict):
            diagnostics = [
                dict(item)
                for item in list(wf_state.get("notebook_synthesis_diagnostics", []) or [])
                if isinstance(item, dict)
            ]
        return build_analysis_notebook(
            workspace,
            conversation_id=conversation_id,
            diagnostics=diagnostics,
        )

    async def _set_active_task_context(
        self,
        conversation_id: str,
        *,
        task_id: str,
        user_prompt: str,
        mode: str,
    ) -> None:
        normalized_mode = _normalize_task_mode(mode)

        def _updater(state: dict) -> dict:
            if not isinstance(state, dict):
                state = {}
            state[STATE_ACTIVE_TASK_CONTEXT] = {
                "conversation_id": conversation_id,
                "task_id": task_id,
                "user_prompt": user_prompt,
                "mode": normalized_mode,
            }
            if normalized_mode == TASK_MODE_ANALYSIS:
                task_state = state.get(STATE_WORKFLOW_TASK)
                if isinstance(task_state, dict):
                    _merge_analysis_runtime_context(
                        task_state,
                        conversation_id=conversation_id,
                        task_id=task_id,
                        user_prompt=user_prompt,
                        mode=normalized_mode,
                    )
                    state[STATE_WORKFLOW_TASK] = task_state
                state[STATE_ANALYSIS_WORKSPACE] = ensure_analysis_workspace(
                    _analysis_workspace_from_session_state(state),
                    conversation_id=conversation_id,
                )
            return state

        await self._update_session_state(conversation_id, _updater)

    async def _read_persistable_session_state(self, conversation_id: str) -> dict:
        return _extract_persistable_session_state(await self._read_session_state(conversation_id))

    async def _persist_conversation_state(self, conversation_id: str, *, task_id: str = "") -> None:
        if not conversation_id:
            return
        state = await self._read_persistable_session_state(conversation_id)
        self.store.save_workflow_session(conversation_id, task_id=task_id, state=state or None)

    async def _is_plan_pending_approval(self, conversation_id: str) -> bool:
        session_state = await self._read_session_state(conversation_id)
        if not session_state:
            return False
        return bool(session_state.get(STATE_PLAN_PENDING_APPROVAL, False))

    async def _run_workflow_turn(
        self,
        conversation_id: str,
        prompt: str,
        *,
        run_id: str,
    ) -> tuple[str, str]:
        """Run a workflow turn in a dedicated thread.

        Returns (response_text, responding_author).
        """
        cs = await self._get_or_create_session(conversation_id)
        main_loop = asyncio.get_running_loop()
        thread_lock = self._get_conv_thread_lock(conversation_id)

        def _thread_target() -> tuple[str, str]:
            acquired = _global_turn_semaphore.acquire(timeout=0)
            if not acquired:
                logger.info("[ui:%s] Turn queued — %d concurrent turns already running (max %d)", run_id, MAX_CONCURRENT_TURNS, MAX_CONCURRENT_TURNS)
                _global_turn_semaphore.acquire()
            try:
                thread_lock.acquire()
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        return loop.run_until_complete(
                            self._workflow_turn_inner(cs, conversation_id, prompt, run_id=run_id, caller_loop=main_loop)
                        )
                    finally:
                        _close_worker_event_loop(loop)
                finally:
                    thread_lock.release()
            finally:
                _global_turn_semaphore.release()

        return await main_loop.run_in_executor(self._thread_pool, _thread_target)

    async def _run_workflow_turn_filtered(
        self,
        conversation_id: str,
        prompt: str,
        *,
        run_id: str,
        max_transient_retries: int = 3,
    ) -> tuple[str, str]:
        """Retry turns when the only returned content is a transient status line."""
        last_response = ("", "")
        for attempt in range(max_transient_retries + 1):
            response_text, responding_author = await self._run_workflow_turn(
                conversation_id,
                prompt,
                run_id=run_id,
            )
            last_response = (response_text, responding_author)
            if not _is_transient_workflow_response(response_text):
                return response_text, responding_author
            await self._append_progress_event(
                run_id,
                phase="execute",
                event_type="model.retry",
                status="progress",
                human_line="Rate limit hit; waiting for the workflow to resume before surfacing output.",
            )
            if attempt < max_transient_retries:
                await asyncio.sleep(0.2)
        return last_response

    async def _workflow_turn_inner(
        self,
        cs: ConversationSession,
        conversation_id: str,
        prompt: str,
        *,
        run_id: str,
        caller_loop: asyncio.AbstractEventLoop,
    ) -> tuple[str, str]:
        """The actual event-processing loop — runs inside its own thread event loop.

        Returns (response_text, responding_author).
        """
        current_message = Content(role="user", parts=[Part(text=prompt)])
        partial_by_author: dict[str, str] = {}
        final_by_author: dict[str, str] = {}
        fallback_chunks: list[str] = []
        step_counter = 0
        planner_seen = False
        step_started_ids: set[str] = set()
        step_completed_ids: set[str] = set()
        tool_error_signatures: set[tuple[str, str, str]] = set()
        saw_notebook_activity = False

        def _step_source_label(tn: str) -> str:
            return _resolve_source_label(tn or "")

        def _fire_progress(**kwargs) -> None:
            asyncio.run_coroutine_threadsafe(
                self._append_progress_event(run_id, **kwargs), caller_loop
            )

        async for event in cs.runner.run_async(
            session_id=cs.session_id,
            user_id=self.user_id,
            new_message=current_message,
        ):
            content = getattr(event, "content", None)
            parts = getattr(content, "parts", None)
            if not parts:
                continue

            if not planner_seen:
                for part in parts:
                    fc = getattr(part, "function_call", None)
                    if fc and getattr(fc, "name", "") == "transfer_to_agent":
                        target = (getattr(fc, "args", None) or {}).get("agent_name", "")
                        if target in {"research_workflow", "analysis_workflow"}:
                            planner_seen = True
                            _fire_progress(
                                phase="plan",
                                event_type="plan.initializing",
                                status="progress",
                                human_line="Preparing a plan...",
                            )
                            break

            author = str(getattr(event, "author", "") or "").strip()
            for part in parts:
                fc = getattr(part, "function_call", None)
                if not fc:
                    continue
                name = str(getattr(fc, "name", "") or "").strip()
                if not name or name == "transfer_to_agent":
                    continue
                args = {}
                try:
                    raw = getattr(fc, "args", None) or {}
                    args = dict(raw) if not isinstance(raw, dict) else raw
                except Exception:  # noqa: BLE001
                    pass
                human = _describe_tool_call(name, args)
                _tool_phase = "synthesize" if author == "analysis_notebook_synthesizer" else "execute"
                _fire_progress(
                    phase=_tool_phase,
                    event_type="tool.called",
                    status="progress",
                    human_line=human,
                    metrics={"tool": name, "author": author},
                )
                # Emit step.started + live step summary so frontend gets tool_log in real time
                if author in {"step_executor", "analysis_step_executor"}:
                    wf_state = await self._read_workflow_state(conversation_id)
                    sid = ""
                    if wf_state:
                        for s in (wf_state.get("steps") or []):
                            st = str(s.get("status", "")).strip()
                            if st == "in_progress":
                                sid = str(s.get("id", "")).strip()
                                break
                        if not sid:
                            for s in (wf_state.get("steps") or []):
                                if str(s.get("status", "")).strip() != "completed":
                                    sid = str(s.get("id", "")).strip()
                                    break
                    if sid and sid not in step_started_ids:
                        step_started_ids.add(sid)
                        step_goal = ""
                        if wf_state:
                            for s in (wf_state.get("steps") or []):
                                if str(s.get("id", "")) == sid:
                                    step_goal = str(s.get("goal", "")).strip()
                                    break
                        human_step = f"{sid}: {step_goal}" if step_goal else f"Executing {sid}…"
                        _fire_progress(
                            phase="execute",
                            event_type="step.started",
                            status="progress",
                            human_line=human_step,
                            metrics={"step_id": sid},
                        )
                    # Emit a live summary snapshot so the frontend has step_details + tool_log
                    if wf_state:
                        _fire_and_forget_threadsafe(
                            self._emit_step_summary(run_id, wf_state, step_counter),
                            caller_loop,
                            label=f"emit_step_summary:{run_id}",
                        )
            for part in parts:
                fr = getattr(part, "function_response", None)
                if not fr:
                    continue
                error_metrics = _extract_tool_error_metrics(fr)
                if error_metrics:
                    signature = (
                        str(error_metrics.get("tool", "") or "").strip(),
                        str(error_metrics.get("error_type", "") or "").strip(),
                        str(error_metrics.get("message", "") or "").strip(),
                    )
                    if signature not in tool_error_signatures:
                        tool_error_signatures.add(signature)
                        tool_name = str(error_metrics.get("tool", "") or "").strip()
                        source = _step_source_label(tool_name) if tool_name else ""
                        error_message = str(error_metrics.get("message", "") or "").strip()
                        if source:
                            human = f"{source} error: {error_message or 'tool call failed.'}"
                        elif tool_name:
                            human = f"{tool_name} error: {error_message or 'tool call failed.'}"
                        else:
                            human = error_message or "Tool call failed."
                        _fire_progress(
                            phase="execute",
                            event_type="tool.failed",
                            status="error",
                            human_line=human,
                            metrics=error_metrics,
                        )
                    continue
                notebook_outcome = _notebook_synthesizer_tool_outcome(fr, author=author)
                if notebook_outcome:
                    saw_notebook_activity = True
                    _fire_progress(
                        phase="synthesize",
                        event_type=str(notebook_outcome.get("event_type") or "notebook.tool.completed"),
                        status=str(notebook_outcome.get("status") or "done"),
                        human_line=str(notebook_outcome.get("human_line") or ""),
                        metrics=notebook_outcome.get("metrics") or {},
                    )

            text = _visible_event_text(parts)
            if not text:
                continue
            if _is_transient_workflow_response(text):
                continue
            if not author:
                continue

            if author in {"step_executor", "analysis_step_executor"}:
                accumulated = f"{partial_by_author.get(author, '')}{text}"
                matches = re.findall(r"###\s*(S\d+)", accumulated)
                if matches:
                    sid = matches[-1]
                    if sid not in step_started_ids:
                        step_started_ids.add(sid)
                        wf_state = await self._read_workflow_state(conversation_id)
                        step_goal = ""
                        if wf_state:
                            for s in (wf_state.get("steps") or []):
                                if str(s.get("id", "")) == sid:
                                    step_goal = str(s.get("goal", "")).strip()
                                    break
                        human = f"{sid}: {step_goal}" if step_goal else f"Executing {sid}…"
                        _fire_progress(
                            phase="execute",
                            event_type="step.started",
                            status="progress",
                            human_line=human,
                            metrics={"step_id": sid},
                        )

            if author in {"step_executor", "analysis_step_executor"} and not bool(getattr(event, "partial", False)):
                metrics = _build_step_completed_event_metrics(text)
                if not metrics:
                    logger.debug(
                        "[ui:%s] ignoring non-step step_executor text chunk: %s",
                        run_id,
                        _compact_text(text, max_chars=120),
                    )
                    continue
                step_id = str(metrics["step_id"]).strip()
                if step_id in step_completed_ids:
                    logger.debug("[ui:%s] suppressing duplicate completion event for %s", run_id, step_id)
                    continue
                step_completed_ids.add(step_id)
                step_counter += 1

                goal = str(metrics.get("goal", "") or "").strip()
                headline = f"{step_id} · {goal}" if goal else f"{step_id} complete"
                _fire_progress(
                    phase="execute",
                    event_type="step.completed",
                    status="done",
                    human_line=_compact_text(headline, max_chars=220),
                    metrics={**metrics, "step_number": step_counter},
                )

                # Emit intermediate step summary so frontend gets tool_log data mid-run
                wf_snap = await self._read_workflow_state(conversation_id)
                if wf_snap:
                    _fire_and_forget_threadsafe(
                        self._emit_step_summary(run_id, wf_snap, step_counter),
                        caller_loop,
                        label=f"emit_step_summary:{run_id}",
                    )

            elif author in {"planner", "analysis_planner"} and not bool(getattr(event, "partial", False)):
                _fire_progress(
                    phase="plan",
                    event_type="plan.generated",
                    status="done",
                    human_line="Research plan generated.",
                )

            elif author in {"report_synthesizer", "analysis_notebook_synthesizer"} and not bool(getattr(event, "partial", False)):
                if author == "analysis_notebook_synthesizer":
                    saw_notebook_activity = True
                _synth_done_line = (
                    "Analysis notebook synthesis completed (check notebook + research log for cell tools)."
                    if author == "analysis_notebook_synthesizer"
                    else "Final report synthesized."
                )
                _fire_progress(
                    phase="synthesize",
                    event_type="synthesis.completed",
                    status="done",
                    human_line=_synth_done_line,
                )

            if bool(getattr(event, "partial", False)):
                partial_by_author[author] = f"{partial_by_author.get(author, '')}{text}"
                continue

            is_terminal_error = _is_terminal_workflow_error_response(text)
            if is_terminal_error:
                partial_by_author.pop(author, None)
                fallback_chunks.clear()

            fallback_chunks.append(text)

            is_final = getattr(event, "is_final_response", None)
            if callable(is_final) and bool(is_final()):
                merged_text = (
                    text
                    if is_terminal_error
                    else (f"{partial_by_author.pop(author, '')}{text}".strip() or text)
                )
                final_by_author[author] = merged_text
                continue
            partial_by_author[author] = f"{partial_by_author.get(author, '')}{text}"

        wf_state = await self._read_workflow_state(conversation_id)
        if wf_state and step_counter > 0:
            _fire_and_forget_threadsafe(
                self._emit_step_summary(run_id, wf_state, step_counter),
                caller_loop,
                label=f"emit_step_summary:{run_id}",
            )
        if wf_state and saw_notebook_activity:
            diagnostics = wf_state.get("notebook_synthesis_diagnostics")
            if isinstance(diagnostics, list) and diagnostics:
                for diag in diagnostics:
                    if not isinstance(diag, dict):
                        continue
                    human = str(diag.get("human_line") or "").strip()
                    if not human:
                        continue
                    await self._append_progress_event(
                        run_id,
                        phase="synthesize",
                        event_type=str(diag.get("event_type") or "notebook.diagnostic"),
                        status=str(diag.get("status") or "progress"),
                        human_line=human,
                        metrics=dict(diag.get("metrics") or {}),
                    )

        _preferred_authors = (
            "analysis_notebook_synthesizer",
            "report_synthesizer",
            "general_qa",
            "clarifier",
            "report_assistant",
            "analysis_workflow",
            "research_workflow",
            "co_scientist_router",
        )
        for preferred_author in _preferred_authors:
            candidate = final_by_author.get(preferred_author, "").strip()
            if candidate:
                return candidate, preferred_author
        if final_by_author:
            for author in sorted(final_by_author.keys(), reverse=True):
                candidate = final_by_author.get(author, "").strip()
                if candidate:
                    return candidate, author
        fallback = "\n".join(chunk for chunk in fallback_chunks if chunk).strip() or "(No response)"
        return fallback, ""

    async def _emit_step_summary(self, run_id: str, wf_state: dict, steps_executed: int) -> None:
        """Build a structured progress summary from workflow state after execution."""
        steps = wf_state.get("steps", [])
        completed = sum(1 for s in steps if s.get("status") == "completed")
        total = len(steps)
        plan_status = wf_state.get("plan_status", "")

        completed_lines = []
        for s in steps:
            if s.get("status") != "completed":
                continue
            sid = s.get("id", "?")
            goal = _compact_text(s.get("goal", ""), max_chars=80)
            completed_lines.append(f"{sid}: {goal}")

        next_lines = []
        for s in steps:
            if s.get("status") == "pending":
                sid = s.get("id", "?")
                goal = _compact_text(s.get("goal", ""), max_chars=80)
                next_lines.append(f"{sid}: {goal}")
                if len(next_lines) >= 2:
                    break

        if plan_status == "completed":
            headline = f"All {total} steps complete — generating report"
        else:
            headline = f"Completed {completed}/{total} steps"

        step_details = []
        for s in steps:
            tool_hint = str(s.get("tool_hint", "")).strip()
            source = _resolve_source_label(tool_hint) if tool_hint else ""
            step_details.append({
                "id": s.get("id", ""),
                "goal": s.get("goal", ""),
                "status": s.get("status", "pending"),
                "tool_hint": tool_hint,
                "source": source,
                "data_sources": _derive_step_data_sources(s),
                "result_summary": s.get("result_summary", ""),
                "tool_reasoning": s.get("tool_reasoning", ""),
                "evidence_ids": s.get("evidence_ids", []),
                "tools_called": s.get("tools_called", []),
                "open_gaps": s.get("open_gaps", []),
                "reasoning_trace": s.get("reasoning_trace", ""),
                "tool_log": s.get("tool_log", []),
                "structured_observations": s.get("structured_observations", []),
            })

        summary = {
            "snapshot_id": f"snap_{uuid.uuid4().hex[:10]}",
            "at": _utc_now(),
            "phase": "synthesize" if plan_status == "completed" else "execute",
            "trigger_type": "step.completed",
            "headline": headline,
            "summary": f"{completed}/{total} plan steps executed",
            "completed": completed_lines[:6],
            "next": next_lines[:3],
            "confidence": "high" if plan_status == "completed" else "medium",
            "step_details": step_details,
            "steps_completed": completed,
            "steps_total": total,
        }
        async with self.runs_lock:
            run = self.runs.get(run_id)
            if run:
                run.progress_summaries.append(summary)
                run.updated_at = _utc_now()

    async def _save_task_with_progress(
        self, task: dict, run_id: str | None = None, *, owner_ip: str = "", merge_progress: bool = True, flush: bool = True
    ) -> None:
        """Save task to store, syncing progress data from the active run unless merge_progress=False."""
        active_run_id = ""
        if run_id and merge_progress:
            async with self.runs_lock:
                run = self.runs.get(run_id)
                if run:
                    task["progress_events"] = list(run.progress_events[-600:])
                    task["progress_summaries"] = list(run.progress_summaries[-80:])
                    if run.status in self._ACTIVE_RUN_STATUSES:
                        active_run_id = run.run_id
        elif run_id:
            async with self.runs_lock:
                run = self.runs.get(run_id)
                if run and run.status in self._ACTIVE_RUN_STATUSES:
                    active_run_id = run.run_id
        if active_run_id:
            task["active_run_id"] = active_run_id
        else:
            task.pop("active_run_id", None)
        conversation_id = str(task.get("conversation_id", "") or "").strip()
        if _normalize_task_mode(task.get("mode")) == TASK_MODE_ANALYSIS and conversation_id:
            workspace = await self._read_analysis_workspace(conversation_id)
            _sync_task_analysis_workspace_fields(task, workspace)
        else:
            _sync_task_analysis_workspace_fields(task, None)
        self.store.save_task(task, owner_ip=owner_ip, flush=flush)
        if flush and conversation_id:
            await self._persist_conversation_state(
                conversation_id,
                task_id=str(task.get("task_id", "") or "").strip(),
            )

    # -- Run management -------------------------------------------------------

    async def _create_run(self, kind: str, *, query: str = "", task_id: str | None = None) -> RunRecord:
        run = RunRecord(
            run_id=f"run_{uuid.uuid4().hex[:10]}",
            kind=kind,
            query=query,
            title=_generate_chat_title(query) if query.strip() else "",
            task_id=task_id,
        )
        async with self.runs_lock:
            self.runs[run.run_id] = run
        self.store.save_run(run.to_dict(), flush=True)
        return run

    async def _update_run(self, run_id: str, **updates) -> None:
        run_payload: dict | None = None
        async with self.runs_lock:
            run = self.runs.get(run_id)
            if not run:
                return
            for key, value in updates.items():
                setattr(run, key, value)
            run.updated_at = _utc_now()
            run_payload = run.to_dict()
        if run_payload:
            self.store.save_run(run_payload, flush=True)

    async def _append_progress_event(
        self,
        run_id: str,
        *,
        phase: str,
        event_type: str,
        status: str,
        human_line: str,
        task_id: str | None = None,
        metrics: dict | None = None,
    ) -> None:
        event = {
            "event_id": f"evt_{uuid.uuid4().hex[:10]}",
            "at": _utc_now(),
            "phase": phase,
            "type": event_type,
            "status": status,
            "human_line": _compact_text(human_line, max_chars=220),
            "task_id": task_id or "",
            "step_index": None,
            "step_title": "",
            "tool": "",
            "metrics": metrics or {},
        }
        run_payload: dict | None = None
        async with self.runs_lock:
            run = self.runs.get(run_id)
            if not run:
                return
            if task_id and not run.task_id:
                run.task_id = task_id
            run.progress_events.append(event)
            if len(run.progress_events) > 600:
                run.progress_events = run.progress_events[-600:]
            if event["human_line"]:
                run.logs.append({"at": event["at"], "message": event["human_line"]})
                if len(run.logs) > 300:
                    run.logs = run.logs[-300:]
            run.updated_at = _utc_now()
            run_payload = run.to_dict()
        if run_payload:
            self.store.save_run(run_payload)
        logger.info("[ui:%s] %s", run_id, event["human_line"])

    def _track_background_task(self, task: asyncio.Task) -> None:
        self.background_tasks.add(task)
        task.add_done_callback(lambda done: self.background_tasks.discard(done))

    # -- Execution flows -------------------------------------------------------

    async def start_new_query(
        self,
        query: str,
        *,
        conversation_id: str | None = None,
        parent_task_id: str | None = None,
        mode: str = TASK_MODE_REPORT,
        owner_ip: str = "",
    ) -> RunRecord:
        run = await self._create_run("new_query", query=query)
        job = asyncio.create_task(
            self._run_new_query(
                run.run_id,
                query,
                conversation_id=conversation_id,
                parent_task_id=parent_task_id,
                mode=mode,
                owner_ip=owner_ip,
            )
        )
        self._track_background_task(job)
        return run

    async def start_task(self, task_id: str) -> RunRecord:
        run = await self._create_run("start_task", task_id=task_id)
        job = asyncio.create_task(self._run_start_task(run.run_id, task_id))
        self._track_background_task(job)
        return run

    async def feedback_task(self, task_id: str, message: str) -> RunRecord:
        run = await self._create_run("feedback_task", task_id=task_id, query=message)
        job = asyncio.create_task(self._run_feedback_task(run.run_id, task_id, message))
        self._track_background_task(job)
        return run

    async def _run_new_query(
        self,
        run_id: str,
        query: str,
        *,
        conversation_id: str | None = None,
        parent_task_id: str | None = None,
        mode: str = TASK_MODE_REPORT,
        owner_ip: str = "",
    ) -> None:
        await self._update_run(run_id, status="running")
        if not self.ready:
            await self._update_run(run_id, status="failed", error=self.ready_error or "Not ready.")
            return

        try:
            task_id = f"task_{uuid.uuid4().hex[:10]}"
            conv_id = conversation_id or f"conv_{task_id}"
            normalized_mode = _normalize_task_mode(mode)
            await self._get_or_create_session(conv_id, mode=normalized_mode)

            title = _generate_chat_title(query)
            parent = parent_task_id.strip() if parent_task_id else None
            branch_label = f"Branched from report {parent}" if parent else ""

            task = _make_task(
                task_id,
                query,
                conv_id,
                title=title,
                user_query=query,
                parent_task_id=parent,
                mode=normalized_mode,
            )
            task["branch_label"] = branch_label
            await self._save_task_with_progress(task, run_id, owner_ip=owner_ip)
            await self._update_run(run_id, task_id=task_id, title=title)
            await self._set_active_task_context(
                conv_id,
                task_id=task_id,
                user_prompt=query,
                mode=normalized_mode,
            )

            _DIRECT_RESPONSE_AGENTS = {"general_qa", "clarifier", "report_assistant"}

            max_plan_attempts = 2
            responding_author = ""
            planner_failed = False
            plan_pending = False

            response_text, responding_author = await self._run_workflow_turn_filtered(
                conv_id, query, run_id=run_id,
            )

            wf_state = await self._read_workflow_state(conv_id)
            plan_pending = await self._is_plan_pending_approval(conv_id)
            has_steps = bool(_steps_from_workflow_state(wf_state))
            terminal_error = _is_terminal_workflow_error_response(response_text)

            direct_response_detected = (
                not terminal_error
                and (
                    responding_author in _DIRECT_RESPONSE_AGENTS
                    or (not wf_state and not plan_pending and not has_steps)
                )
            )

            if not terminal_error and not direct_response_detected:
                response_text, responding_author, wf_state = await self._retry_analysis_notebook_stage_if_needed(
                    conversation_id=conv_id,
                    task_id=task_id,
                    run_id=run_id,
                    task=task,
                    response_text=response_text,
                    responding_author=responding_author,
                    wf_state=wf_state,
                )
                plan_pending = await self._is_plan_pending_approval(conv_id)
                has_steps = bool(_steps_from_workflow_state(wf_state))
                terminal_error = _is_terminal_workflow_error_response(response_text)
                direct_response_detected = (
                    not terminal_error
                    and (
                        responding_author in _DIRECT_RESPONSE_AGENTS
                        or (not wf_state and not plan_pending and not has_steps)
                    )
                )

            if not terminal_error and not direct_response_detected:
                await self._append_progress_event(
                    run_id,
                    phase="intake",
                    event_type="run.started",
                    status="start",
                    human_line=f"Started: {title}",
                    task_id=task_id,
                )
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="plan.initializing",
                    status="progress",
                    human_line="Building research plan...",
                    task_id=task_id,
                )

                task["steps"] = _steps_from_workflow_state(wf_state)
                task["current_step_index"] = 0

                restated = (wf_state or {}).get("objective", "").strip()
                if restated:
                    task["objective"] = restated
                    task["title"] = _generate_chat_title(restated)

                planner_failed = not wf_state and not plan_pending
                if planner_failed:
                    for plan_attempt in range(2, max_plan_attempts + 1):
                        logger.warning(
                            "[new_task] Planner failed (attempt %d/%d), retrying...",
                            plan_attempt - 1, max_plan_attempts,
                        )
                        await self._append_progress_event(
                            run_id,
                            phase="plan",
                            event_type="plan.retry",
                            status="progress",
                            human_line=f"Plan generation failed (attempt {plan_attempt - 1}), retrying...",
                            task_id=task_id,
                        )
                        response_text, responding_author = await self._run_workflow_turn_filtered(
                            conv_id, query, run_id=run_id,
                        )
                        wf_state = await self._read_workflow_state(conv_id)
                        plan_pending = await self._is_plan_pending_approval(conv_id)
                        task["steps"] = _steps_from_workflow_state(wf_state)
                        task["current_step_index"] = 0
                        restated = (wf_state or {}).get("objective", "").strip()
                        if restated:
                            task["objective"] = restated
                            task["title"] = _generate_chat_title(restated)
                        planner_failed = not wf_state and not plan_pending
                        if not planner_failed:
                            break

            if terminal_error:
                run_error = _derive_run_error_message(response_text, "Run failed.")
                task["status"] = "failed"
                task["report_markdown"] = response_text
                _sync_task_dataset_visualizations(task, wf_state)
                await self._save_task_with_progress(task, run_id)
                await self._update_run(
                    run_id, status="failed", task_id=task_id,
                    error=run_error,
                )
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="run.failed",
                    status="error",
                    human_line=run_error,
                    task_id=task_id,
                )

            elif direct_response_detected:
                task["status"] = "completed"
                task["is_direct_response"] = True
                task["direct_response_text"] = response_text
                task["progress_events"] = []
                task["progress_summaries"] = []
                task["dataset_visualizations"] = None
                await self._save_task_with_progress(task, run_id, merge_progress=False)
                async with self.runs_lock:
                    run = self.runs.get(run_id)
                    if run:
                        run.progress_events.clear()
                        run.progress_summaries.clear()
                await self._update_run(
                    run_id, status="completed", task_id=task_id,
                    final_report=response_text,
                )

            elif planner_failed:
                planner_error = _derive_run_error_message(
                    response_text,
                    "Planner failed to generate a valid research plan.",
                )
                task["status"] = "failed"
                task["report_markdown"] = response_text
                _sync_task_dataset_visualizations(task, wf_state)
                await self._save_task_with_progress(task, run_id)
                await self._update_run(
                    run_id, status="failed", task_id=task_id,
                    error=planner_error,
                )
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="plan.failed",
                    status="error",
                    human_line=planner_error,
                    task_id=task_id,
                )
            elif plan_pending:
                task["awaiting_hitl"] = True
                task["status"] = "in_progress"
                _sync_task_dataset_visualizations(task, wf_state)
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="task.created",
                    status="done",
                    human_line=f"Plan ready with {len(task['steps'])} steps. Waiting for approval.",
                    task_id=task_id,
                    metrics={"steps_total": len(task["steps"])},
                )
                await self._save_task_with_progress(task, run_id)
                await self._update_run(run_id, status="awaiting_hitl", task_id=task_id)
            else:
                task["status"] = "completed"
                task["follow_up_suggestions"] = _extract_next_steps(response_text)
                task["report_markdown"] = _strip_next_steps_section(response_text)
                _sync_task_dataset_visualizations(task, wf_state)
                await self._save_task_with_progress(task, run_id)
                await self._update_run(
                    run_id, status="completed", task_id=task_id,
                    final_report=task["report_markdown"],
                )
                await self._append_progress_event(
                    run_id,
                    phase="finalize",
                    event_type="run.completed",
                    status="done",
                    human_line="Research complete.",
                    task_id=task_id,
                )

        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            await self._append_progress_event(
                run_id,
                phase="finalize",
                event_type="run.failed",
                status="error",
                human_line=f"Run failed: {error}",
            )
            await self._update_run(run_id, status="failed", error=error)
            traceback.print_exc()

    async def _run_start_task(self, run_id: str, task_id: str) -> None:
        await self._update_run(run_id, status="running")
        if not self.ready:
            await self._update_run(run_id, status="failed", error=self.ready_error or "Not ready.")
            return

        try:
            task = self.store.get_task(task_id)
            if not task:
                await self._update_run(run_id, status="failed", error=f"Task {task_id} not found.")
                return
            if not task.get("awaiting_hitl"):
                await self._update_run(run_id, status="failed", error="Task is not at checkpoint.")
                return

            conv_id = task["conversation_id"]
            await self._get_or_create_session(conv_id, mode=_normalize_task_mode(task.get("mode")))
            await self._update_run(run_id, task_id=task_id, title=task.get("title", ""))
            task["hitl_history"].append("approve")
            task["awaiting_hitl"] = False
            await self._save_task_with_progress(task, run_id)
            await self._set_active_task_context(
                conv_id,
                task_id=task_id,
                user_prompt=str(task.get("user_query", "") or task.get("objective", "") or "").strip(),
                mode=_normalize_task_mode(task.get("mode")),
            )

            await self._append_progress_event(
                run_id,
                phase="execute",
                event_type="execution.running",
                status="start",
                human_line="Executing plan...",
                task_id=task_id,
            )

            max_continue_loops = 100
            for loop_idx in range(max_continue_loops):
                response_text, _ = await self._run_workflow_turn_filtered(
                    conv_id, "approve", run_id=run_id,
                )
                if _is_terminal_workflow_error_response(response_text):
                    run_error = _derive_run_error_message(response_text, "Run failed.")
                    task["status"] = "failed"
                    task["report_markdown"] = response_text
                    await self._save_task_with_progress(task, run_id)
                    await self._append_progress_event(
                        run_id,
                        phase="execute",
                        event_type="run.failed",
                        status="error",
                        human_line=run_error,
                        task_id=task_id,
                    )
                    await self._update_run(
                        run_id, status="failed", task_id=task_id, error=run_error,
                    )
                    break

                wf_state = await self._read_workflow_state(conv_id)
                plan_pending = await self._is_plan_pending_approval(conv_id)
                response_text, _, wf_state = await self._retry_analysis_notebook_stage_if_needed(
                    conversation_id=conv_id,
                    task_id=task_id,
                    run_id=run_id,
                    task=task,
                    response_text=response_text,
                    responding_author="",
                    wf_state=wf_state,
                )
                if _is_terminal_workflow_error_response(response_text):
                    run_error = _derive_run_error_message(response_text, "Run failed.")
                    task["status"] = "failed"
                    task["report_markdown"] = response_text
                    await self._save_task_with_progress(task, run_id)
                    await self._append_progress_event(
                        run_id,
                        phase="execute",
                        event_type="run.failed",
                        status="error",
                        human_line=run_error,
                        task_id=task_id,
                    )
                    await self._update_run(
                        run_id, status="failed", task_id=task_id, error=run_error,
                    )
                    break
                plan_pending = await self._is_plan_pending_approval(conv_id)

                task["steps"] = _steps_from_workflow_state(wf_state)
                completed_steps = sum(
                    1 for s in task["steps"] if s.get("status") == "completed"
                )
                pending_steps = sum(
                    1 for s in task["steps"] if str(s.get("status", "")).strip() == "pending"
                )
                task["current_step_index"] = completed_steps

                plan_status = wf_state.get("plan_status", "") if wf_state else ""
                has_synthesis = bool(
                    wf_state
                    and wf_state.get("latest_synthesis", {})
                    and wf_state["latest_synthesis"].get("markdown", "").strip()
                )
                terminal_plan = plan_status == "completed" or (not pending_steps and has_synthesis)
                _sync_task_dataset_visualizations(task, wf_state)

                if terminal_plan and has_synthesis:
                    task["status"] = "completed"
                    final_md = wf_state["latest_synthesis"]["markdown"]
                    task["follow_up_suggestions"] = _extract_next_steps(final_md)
                    stripped_md = _strip_next_steps_section(final_md)
                    task["report_markdown"] = stripped_md
                    await self._append_progress_event(
                        run_id,
                        phase="finalize",
                        event_type="run.completed",
                        status="done",
                        human_line="Report completed.",
                        task_id=task_id,
                    )
                    await self._save_task_with_progress(task, run_id)
                    self._write_report(task_id, stripped_md)
                    await self._update_run(
                        run_id, status="completed", task_id=task_id,
                        final_report=stripped_md,
                        follow_up_suggestions=task["follow_up_suggestions"],
                    )
                    break

                if plan_pending:
                    task["awaiting_hitl"] = True
                    task["status"] = "in_progress"
                    await self._save_task_with_progress(task, run_id)
                    await self._update_run(run_id, status="awaiting_hitl", task_id=task_id)
                    await self._append_progress_event(
                        run_id,
                        phase="checkpoint",
                        event_type="checkpoint.opened",
                        status="done",
                        human_line="Revised plan ready. Waiting for approval.",
                        task_id=task_id,
                    )
                    break

                if not terminal_plan:
                    await self._save_task_with_progress(task, run_id, flush=False)
                    await self._append_progress_event(
                        run_id,
                        phase="execute",
                        event_type="execution.running",
                        status="progress",
                        human_line=f"Executing step {completed_steps + 1}/{len(task['steps'])}...",
                        task_id=task_id,
                    )
                    continue

                task["status"] = "completed"
                task["follow_up_suggestions"] = _extract_next_steps(response_text)
                stripped_md = _strip_next_steps_section(response_text)
                task["report_markdown"] = stripped_md
                await self._append_progress_event(
                    run_id,
                    phase="finalize",
                    event_type="run.completed",
                    status="done",
                    human_line="Report completed.",
                    task_id=task_id,
                )
                await self._save_task_with_progress(task, run_id)
                self._write_report(task_id, stripped_md)
                await self._update_run(
                    run_id, status="completed", task_id=task_id,
                    final_report=stripped_md,
                    follow_up_suggestions=task["follow_up_suggestions"],
                )
                break

        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            await self._append_progress_event(
                run_id,
                phase="finalize",
                event_type="run.failed",
                status="error",
                human_line=f"Run failed: {error}",
                task_id=task_id,
            )
            await self._update_run(run_id, status="failed", error=error)
            traceback.print_exc()

    async def _run_feedback_task(self, run_id: str, task_id: str, message: str) -> None:
        await self._update_run(run_id, status="running")
        if not self.ready:
            await self._update_run(run_id, status="failed", error=self.ready_error or "Not ready.")
            return

        try:
            task = self.store.get_task(task_id)
            if not task:
                await self._update_run(run_id, status="failed", error=f"Task {task_id} not found.")
                return

            conv_id = task["conversation_id"]
            await self._get_or_create_session(conv_id, mode=_normalize_task_mode(task.get("mode")))
            await self._update_run(run_id, task_id=task_id, title=task.get("title", ""))
            await self._set_active_task_context(
                conv_id,
                task_id=task_id,
                user_prompt=str(task.get("user_query", "") or task.get("objective", "") or "").strip(),
                mode=_normalize_task_mode(task.get("mode")),
            )

            prompt = f"revise: {message}"
            await self._append_progress_event(
                run_id,
                phase="plan",
                event_type="feedback.applying",
                status="start",
                human_line="Applying feedback...",
                task_id=task_id,
            )

            response_text, _ = await self._run_workflow_turn_filtered(
                conv_id, prompt, run_id=run_id,
            )
            if _is_terminal_workflow_error_response(response_text):
                run_error = _derive_run_error_message(response_text, "Run failed.")
                task["status"] = "failed"
                task["report_markdown"] = response_text
                await self._save_task_with_progress(task, run_id)
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="run.failed",
                    status="error",
                    human_line=run_error,
                    task_id=task_id,
                )
                await self._update_run(
                    run_id, status="failed", task_id=task_id, error=run_error,
                )
                return

            wf_state = await self._read_workflow_state(conv_id)
            plan_pending = await self._is_plan_pending_approval(conv_id)
            response_text, _, wf_state = await self._retry_analysis_notebook_stage_if_needed(
                conversation_id=conv_id,
                task_id=task_id,
                run_id=run_id,
                task=task,
                response_text=response_text,
                responding_author="",
                wf_state=wf_state,
            )
            if _is_terminal_workflow_error_response(response_text):
                run_error = _derive_run_error_message(response_text, "Run failed.")
                task["status"] = "failed"
                task["report_markdown"] = response_text
                await self._save_task_with_progress(task, run_id)
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="run.failed",
                    status="error",
                    human_line=run_error,
                    task_id=task_id,
                )
                await self._update_run(
                    run_id, status="failed", task_id=task_id, error=run_error,
                )
                return
            plan_pending = await self._is_plan_pending_approval(conv_id)

            task["steps"] = _steps_from_workflow_state(wf_state)
            task["hitl_history"].append(f"revise:{message}")
            task["awaiting_hitl"] = plan_pending
            task["status"] = "in_progress"

            restated = (wf_state or {}).get("objective", "").strip()
            if restated:
                task["objective"] = restated
                task["title"] = _generate_chat_title(restated)
                await self._update_run(run_id, title=task["title"])

            await self._save_task_with_progress(task, run_id)

            if plan_pending:
                await self._update_run(run_id, status="awaiting_hitl", task_id=task_id)
                await self._append_progress_event(
                    run_id,
                    phase="checkpoint",
                    event_type="feedback.applied",
                    status="done",
                    human_line="Revised plan ready. Waiting for approval.",
                    task_id=task_id,
                )
            else:
                await self._update_run(run_id, status="completed", task_id=task_id)
                await self._append_progress_event(
                    run_id,
                    phase="plan",
                    event_type="feedback.applied",
                    status="done",
                    human_line="Feedback applied.",
                    task_id=task_id,
                )

        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            await self._append_progress_event(
                run_id,
                phase="finalize",
                event_type="run.failed",
                status="error",
                human_line=f"Run failed: {error}",
                task_id=task_id,
            )
            await self._update_run(run_id, status="failed", error=error)
            traceback.print_exc()

    # -- Reports ---------------------------------------------------------------

    def _report_dir(self) -> Path:
        d = Path(__file__).resolve().parent / "reports"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _write_report(self, task_id: str, markdown: str) -> Path:
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", task_id)
        path = self._report_dir() / f"{safe_id}.md"
        path.write_text((markdown or "").rstrip() + "\n", encoding="utf-8")
        return path

    # -- Read APIs (conversations, tasks, runs) --------------------------------

    _ACTIVE_RUN_STATUSES = frozenset({"running", "queued", "awaiting_hitl", "in_progress"})

    async def _overlay_live_progress(self, task: dict) -> dict:
        """Merge live progress from a run into the task dict. Adds active_run_id when run is in progress."""
        task_id = task.get("task_id", "")
        if not task_id:
            return task
        async with self.runs_lock:
            best: RunRecord | None = None
            for run in self.runs.values():
                if run.task_id != task_id:
                    continue
                if best is None or run.updated_at > best.updated_at:
                    best = run
            if best is not None:
                task = dict(task)
                if best.status in self._ACTIVE_RUN_STATUSES:
                    task["active_run_id"] = best.run_id
                if best.progress_events:
                    task["progress_events"] = list(best.progress_events)
                if best.progress_summaries:
                    task["progress_summaries"] = list(best.progress_summaries)
        return task

    def list_conversations(self, *, owner_ip: str = "") -> list[dict]:
        return self.store.list_conversations(owner_ip=owner_ip)

    async def get_conversation_detail(self, conversation_id: str) -> dict | None:
        tasks = self.store.get_conversation_tasks(conversation_id)
        if not tasks:
            return None
        tasks.sort(key=lambda t: (t.get("created_at", ""), t.get("task_id", "")))

        tasks = [await self._overlay_live_progress(t) for t in tasks]

        iterations = []
        for idx, task in enumerate(tasks, start=1):
            iterations.append(_iteration_from_task(task, idx))

        latest = max(tasks, key=lambda t: t.get("updated_at", ""))
        root = tasks[0]
        latest_completed = next(
            (t for t in reversed(tasks) if t.get("status") == "completed"), None
        )
        selected_report_task_id = (
            latest_completed["task_id"] if latest_completed else latest["task_id"]
        )

        return {
            "conversation": {
                "conversation_id": conversation_id,
                "title": latest.get("title") or root.get("title") or "Research",
                "mode": _normalize_task_mode(root.get("mode")),
                "root_task_id": root["task_id"],
                "latest_task_id": latest["task_id"],
                "latest_status": latest.get("status", ""),
                "updated_at": latest.get("updated_at", ""),
                "iteration_count": len(tasks),
                "selected_report_task_id": selected_report_task_id,
            },
            "iterations": iterations,
        }

    async def get_conversation_analysis_workspace(self, conversation_id: str) -> dict | None:
        tasks = self.store.get_conversation_tasks(conversation_id)
        if not tasks:
            return None
        tasks.sort(key=lambda t: (t.get("created_at", ""), t.get("task_id", "")))
        live_state = await self._read_persistable_session_state(conversation_id)
        persisted = self.store.get_workflow_session(conversation_id)
        source = "none"
        state: dict[str, object] = {}
        if live_state:
            source = "live"
            state = live_state
        elif isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            source = "persisted"
            state = copy.deepcopy(persisted["state"])

        workspace = _analysis_workspace_from_session_state(state if isinstance(state, dict) else None)
        persisted_workspace = None
        if isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            persisted_workspace = _analysis_workspace_from_session_state(persisted["state"])
        if persisted_workspace and not dict((workspace or {}).get("datasets") or {}):
            workspace = copy.deepcopy(persisted_workspace)
        selected_dataset_snapshot = next(
            (
                str(task.get("selected_dataset_id_snapshot", "") or "").strip()
                for task in reversed(tasks)
                if str(task.get("selected_dataset_id_snapshot", "") or "").strip()
            ),
            "",
        )
        if (
            isinstance(workspace, dict)
            and selected_dataset_snapshot
            and not str(workspace.get("selected_dataset_id") or "").strip()
            and selected_dataset_snapshot in set((workspace.get("datasets") or {}).keys())
        ):
            workspace = copy.deepcopy(workspace)
            workspace["selected_dataset_id"] = selected_dataset_snapshot
        root = tasks[0]
        normalized_mode = _normalize_task_mode(root.get("mode"))
        if normalized_mode == TASK_MODE_ANALYSIS:
            workspace = ensure_analysis_workspace(
                workspace,
                conversation_id=conversation_id,
            )
            workspace, recovered = _recover_analysis_workspace_from_tasks(
                tasks,
                workspace=workspace,
                conversation_id=conversation_id,
            )
            if recovered and isinstance(workspace, dict):
                state = dict(state or {})
                state[STATE_ANALYSIS_WORKSPACE] = copy.deepcopy(workspace)
                notebook_payload = await self._build_analysis_notebook_payload(
                    conversation_id=conversation_id,
                    workspace=workspace,
                    session_state=state,
                )
                if notebook_payload:
                    state[STATE_ANALYSIS_NOTEBOOK] = copy.deepcopy(notebook_payload)
                last_task_id = str(tasks[-1].get("task_id", "") or "").strip()
                if live_state:
                    await self._update_session_state(
                        conversation_id,
                        lambda current_state: {
                            **(current_state or {}),
                            STATE_ANALYSIS_WORKSPACE: copy.deepcopy(workspace),
                            STATE_ANALYSIS_NOTEBOOK: copy.deepcopy(notebook_payload) if notebook_payload else None,
                        },
                    )
                self.store.save_workflow_session(
                    conversation_id,
                    task_id=last_task_id,
                    state=state,
                )
                for task in tasks:
                    updated_task = dict(task)
                    _sync_task_analysis_workspace_fields(updated_task, workspace)
                    self.store.save_task(updated_task, flush=False)
                source = f"{source}+recovered" if source != "none" else "recovered"
        if not workspace:
            return {
                "conversation_id": conversation_id,
                "mode": normalized_mode,
                "source": source,
                "workspace": None,
            }
        return {
            "conversation_id": conversation_id,
            "mode": normalized_mode,
            "source": source,
            "workspace": copy.deepcopy(workspace),
        }

    async def get_conversation_analysis_notebook(self, conversation_id: str) -> dict | None:
        tasks = self.store.get_conversation_tasks(conversation_id)
        if not tasks:
            return None
        tasks.sort(key=lambda t: (t.get("created_at", ""), t.get("task_id", "")))
        live_state = await self._read_persistable_session_state(conversation_id)
        persisted = self.store.get_workflow_session(conversation_id)
        source = "none"
        state: dict[str, object] = {}
        if live_state:
            source = "live"
            state = live_state
        elif isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            source = "persisted"
            state = copy.deepcopy(persisted["state"])

        workspace = _analysis_workspace_from_session_state(state if isinstance(state, dict) else None)
        notebook = _analysis_notebook_from_session_state(state if isinstance(state, dict) else None)
        persisted_workspace = None
        persisted_notebook = None
        if isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            persisted_workspace = _analysis_workspace_from_session_state(persisted["state"])
            persisted_notebook = _analysis_notebook_from_session_state(persisted["state"])
        if persisted_workspace and not dict((workspace or {}).get("datasets") or {}):
            workspace = copy.deepcopy(persisted_workspace)
        if persisted_notebook and not isinstance(notebook, dict):
            notebook = copy.deepcopy(persisted_notebook)
        selected_dataset_snapshot = next(
            (
                str(task.get("selected_dataset_id_snapshot", "") or "").strip()
                for task in reversed(tasks)
                if str(task.get("selected_dataset_id_snapshot", "") or "").strip()
            ),
            "",
        )
        if (
            isinstance(workspace, dict)
            and selected_dataset_snapshot
            and not str(workspace.get("selected_dataset_id") or "").strip()
            and selected_dataset_snapshot in set((workspace.get("datasets") or {}).keys())
        ):
            workspace = copy.deepcopy(workspace)
            workspace["selected_dataset_id"] = selected_dataset_snapshot
        root = tasks[0]
        normalized_mode = _normalize_task_mode(root.get("mode"))
        if normalized_mode == TASK_MODE_ANALYSIS:
            workspace = ensure_analysis_workspace(
                workspace,
                conversation_id=conversation_id,
            )
            workspace, recovered = _recover_analysis_workspace_from_tasks(
                tasks,
                workspace=workspace,
                conversation_id=conversation_id,
            )
            notebook = await self._build_analysis_notebook_payload(
                conversation_id=conversation_id,
                workspace=workspace,
                session_state=state if isinstance(state, dict) else None,
            )
            if notebook:
                state = dict(state or {})
                state[STATE_ANALYSIS_WORKSPACE] = copy.deepcopy(workspace)
                state[STATE_ANALYSIS_NOTEBOOK] = copy.deepcopy(notebook)
                last_task_id = str(tasks[-1].get("task_id", "") or "").strip()
                if live_state:
                    await self._update_session_state(
                        conversation_id,
                        lambda current_state: {
                            **(current_state or {}),
                            STATE_ANALYSIS_WORKSPACE: copy.deepcopy(workspace),
                            STATE_ANALYSIS_NOTEBOOK: copy.deepcopy(notebook),
                        },
                    )
                self.store.save_workflow_session(
                    conversation_id,
                    task_id=last_task_id,
                    state=state,
                )
                if recovered and isinstance(workspace, dict):
                    for task in tasks:
                        updated_task = dict(task)
                        _sync_task_analysis_workspace_fields(updated_task, workspace)
                        self.store.save_task(updated_task, flush=False)
        if not notebook:
            return {
                "conversation_id": conversation_id,
                "mode": normalized_mode,
                "source": source,
                "workspace": copy.deepcopy(workspace) if isinstance(workspace, dict) else None,
                "notebook": None,
                "download_path": None,
            }
        return {
            "conversation_id": conversation_id,
            "mode": normalized_mode,
            "source": source,
            "workspace": copy.deepcopy(workspace) if isinstance(workspace, dict) else None,
            "download_path": f"/api/conversations/{conversation_id}/analysis-notebook.ipynb",
            **notebook_json_for_api(notebook),
        }

    async def set_conversation_selected_dataset(self, conversation_id: str, dataset_id: str) -> dict | None:
        tasks = self.store.get_conversation_tasks(conversation_id)
        if not tasks:
            return None
        root = tasks[0]
        if _normalize_task_mode(root.get("mode")) != TASK_MODE_ANALYSIS:
            raise ValueError("Selected dataset is only available in analysis mode.")

        persisted_session = self.store.get_workflow_session(conversation_id)
        persisted_workspace = None
        if isinstance(persisted_session, dict) and isinstance(persisted_session.get("state"), dict):
            persisted_workspace = _analysis_workspace_from_session_state(persisted_session["state"])

        await self._get_or_create_session(conversation_id, mode=TASK_MODE_ANALYSIS)

        def _updater(state: dict) -> dict:
            if not isinstance(state, dict):
                state = {}
            live_workspace = _analysis_workspace_from_session_state(state)
            workspace_seed = live_workspace
            if persisted_workspace and not dict((live_workspace or {}).get("datasets") or {}):
                workspace_seed = persisted_workspace
            workspace = ensure_analysis_workspace(
                workspace_seed,
                conversation_id=conversation_id,
            )
            if not set_selected_dataset(workspace, dataset_id):
                raise KeyError(dataset_id)
            state[STATE_ANALYSIS_WORKSPACE] = workspace
            state[STATE_ANALYSIS_NOTEBOOK] = build_analysis_notebook(
                workspace,
                conversation_id=conversation_id,
            )
            return state

        try:
            state = await self._update_session_state(conversation_id, _updater)
        except KeyError as exc:
            raise KeyError(str(exc)) from exc
        workspace = _analysis_workspace_from_session_state(state if isinstance(state, dict) else None)
        notebook = _analysis_notebook_from_session_state(state if isinstance(state, dict) else None)
        for task in tasks:
            updated_task = dict(task)
            _sync_task_analysis_workspace_fields(updated_task, workspace)
            self.store.save_task(updated_task, flush=False)
        self.store.save_workflow_session(
            conversation_id,
            task_id=str(tasks[-1].get("task_id", "") or "").strip(),
            state=_extract_persistable_session_state(
                {
                    **(state or {}),
                    STATE_ANALYSIS_WORKSPACE: copy.deepcopy(workspace) if isinstance(workspace, dict) else None,
                    STATE_ANALYSIS_NOTEBOOK: copy.deepcopy(notebook) if isinstance(notebook, dict) else None,
                }
            ),
        )
        await self._persist_conversation_state(
            conversation_id,
            task_id=str(tasks[-1].get("task_id", "") or "").strip(),
        )
        return {
            "conversation_id": conversation_id,
            "selected_dataset_id": str((workspace or {}).get("selected_dataset_id") or "").strip(),
            "workspace_revision": int((workspace or {}).get("revision", 0) or 0),
        }

    async def get_task_detail(self, task_id: str) -> dict | None:
        task = self.store.get_task(task_id)
        if not task:
            return None
        task = await self._overlay_live_progress(task)
        return {
            "task": _task_detail(task),
            "active_plan_version": {
                "version_id": f"plan_{task_id}",
                "steps": _normalize_steps_for_ui(task.get("steps", [])),
            } if task.get("steps") else None,
            "latest_plan_delta": None,
            "pending_feedback_queue_count": 0,
            "checkpoint_reason": "",
            "checkpoint_payload": {},
            "phase_state": {},
            "planner_mode": "",
            "quality_confidence": "",
            "researcher_candidates": [],
            "revisions": [],
            "report_markdown_path": None,
            "report_markdown": task.get("report_markdown", ""),
            "report_pdf_path": None,
        }

    async def get_task_workflow_state_debug(self, task_id: str) -> dict | None:
        task = self.store.get_task(task_id)
        if not task:
            return None
        conversation_id = str(task.get("conversation_id", "") or "").strip()
        live_state = await self._read_persistable_session_state(conversation_id)
        persisted = self.store.get_workflow_session(conversation_id) if conversation_id else None
        source = "none"
        state: dict[str, object] = {}
        if live_state:
            source = "live"
            state = live_state
        elif isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            source = "persisted"
            state = copy.deepcopy(persisted["state"])
        return {
            "task_id": task_id,
            "conversation_id": conversation_id,
            "source": source,
            "state": state,
            "persisted_updated_at": str((persisted or {}).get("updated_at", "") or ""),
            "persisted_task_id": str((persisted or {}).get("task_id", "") or ""),
        }

    async def get_task_evidence_graph(self, task_id: str) -> dict | None:
        task = self.store.get_task(task_id)
        if not task:
            return None
        conversation_id = str(task.get("conversation_id", "") or "").strip()
        live_state = await self._read_persistable_session_state(conversation_id)
        persisted = self.store.get_workflow_session(conversation_id) if conversation_id else None
        source = "none"
        state: dict[str, object] = {}
        if live_state:
            source = "live"
            state = live_state
        elif isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
            source = "persisted"
            state = copy.deepcopy(persisted["state"])

        task_state = state.get(STATE_WORKFLOW_TASK) if isinstance(state, dict) else None
        graph_payload = _build_semantic_evidence_graph(task_state if isinstance(task_state, dict) else {})
        warnings = list(graph_payload.get("warnings", []) or [])
        if source == "none":
            warnings = ["No workflow state is available for this task yet."] + warnings

        return {
            "task_id": task_id,
            "conversation_id": conversation_id,
            "source": source,
            "mode": str(graph_payload.get("mode", "semantic") or "semantic"),
            "summary": dict(graph_payload.get("summary", {}) or {}),
            "warnings": warnings,
            "elements": {
                "nodes": list(((graph_payload.get("elements", {}) or {}).get("nodes", []) or [])),
                "edges": list(((graph_payload.get("elements", {}) or {}).get("edges", []) or [])),
            },
            "persisted_updated_at": str((persisted or {}).get("updated_at", "") or ""),
            "persisted_task_id": str((persisted or {}).get("task_id", "") or ""),
        }

    async def get_task_dataset_visualizations(self, task_id: str) -> dict | None:
        task = self.store.get_task(task_id)
        if not task:
            return None
        conversation_id = str(task.get("conversation_id", "") or "").strip()
        mode = _normalize_task_mode(task.get("mode"))
        bundle = task.get("dataset_visualizations")
        source = "task"

        if not isinstance(bundle, dict) or not list((bundle.get("rows", []) or [])):
            live_state = await self._read_persistable_session_state(conversation_id)
            persisted = self.store.get_workflow_session(conversation_id) if conversation_id else None
            source = "none"
            state: dict[str, object] = {}
            if live_state:
                source = "live"
                state = live_state
            elif isinstance(persisted, dict) and isinstance(persisted.get("state"), dict):
                source = "persisted"
                state = copy.deepcopy(persisted["state"])
            task_state = state.get(STATE_WORKFLOW_TASK) if isinstance(state, dict) else None
            bundle = _dataset_visualizations_from_workflow_state(task_state if isinstance(task_state, dict) else None)

        available = bool(isinstance(bundle, dict) and list((bundle.get("rows", []) or [])))
        return {
            "task_id": task_id,
            "conversation_id": conversation_id,
            "mode": mode,
            "source": source,
            "available": available,
            "visualizations": copy.deepcopy(bundle) if isinstance(bundle, dict) else None,
            "message": (
                "Dataset visual summary is available."
                if available
                else "No dataset visual summary is available for this task yet."
            ),
        }

    async def get_run(self, run_id: str) -> dict | None:
        async with self.runs_lock:
            run = self.runs.get(run_id)
            if not run:
                return self.store.get_run(run_id)
            return run.to_dict()


def _extract_next_steps(markdown: str) -> list[str]:
    """Try to pull next steps / potential next steps from synthesized report."""
    suggestions: list[str] = []
    in_next_steps = False
    for line in str(markdown or "").split("\n"):
        stripped = line.strip()
        lowered = stripped.lower()
        if "next step" in lowered or "potential next" in lowered:
            in_next_steps = True
            continue
        if in_next_steps:
            if stripped.startswith("#"):
                break
            m = re.match(r"^[-*\d.]+\s+(.+)$", stripped)
            if m:
                suggestions.append(m.group(1).strip())
            if len(suggestions) >= 3:
                break
    return suggestions


def _strip_next_steps_section(markdown: str) -> str:
    """Remove the Next Steps / Potential Next Steps heading and its content."""
    lines = str(markdown or "").split("\n")
    out: list[str] = []
    skipping = False
    for line in lines:
        stripped = line.strip()
        lowered = stripped.lower()
        if stripped.startswith("#") and ("next step" in lowered or "potential next" in lowered):
            skipping = True
            continue
        if skipping:
            if stripped.startswith("#"):
                skipping = False
            else:
                continue
        out.append(line)
    return "\n".join(out).rstrip()


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parent
UI_DIR = ROOT_DIR / "ui"
STATE_PATH = ROOT_DIR / "state" / "workflow_tasks.json"

runtime = UiRuntime(STATE_PATH)
app = FastAPI(title="AI Co-Scientist UI", version="0.2.0")
app.mount("/static", StaticFiles(directory=str(UI_DIR)), name="static")


def _ga4_head_snippet() -> str:
    if not GA4_MEASUREMENT_ID:
        return ""
    if not _GA4_ID_PATTERN.fullmatch(GA4_MEASUREMENT_ID):
        logger.warning("Ignoring invalid GA4_MEASUREMENT_ID format.")
        return ""
    ga_id = GA4_MEASUREMENT_ID
    return (
        f'<script async src="https://www.googletagmanager.com/gtag/js?id={ga_id}"></script>\n'
        "<script>\n"
        "  window.dataLayer = window.dataLayer || [];\n"
        "  function gtag(){dataLayer.push(arguments);}\n"
        "  gtag('js', new Date());\n"
        f"  gtag('config', '{ga_id}', {{ anonymize_ip: true }});\n"
        "</script>"
    )


def _ui_asset_version() -> str:
    parts: list[str] = []
    for name in ("app.js", "styles.css"):
        try:
            parts.append(str(int((UI_DIR / name).stat().st_mtime)))
        except OSError:
            continue
    return "-".join(parts) or str(int(time.time()))


def _render_ui_page(filename: str) -> HTMLResponse:
    html_path = UI_DIR / filename
    html = html_path.read_text(encoding="utf-8")
    html = html.replace("<!-- GA4_SNIPPET -->", _ga4_head_snippet(), 1)
    html = html.replace("__UI_VERSION__", _ui_asset_version())
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.on_event("startup")
async def _startup() -> None:
    await runtime.startup()
    if runtime.ready:
        _port = int(os.environ.get("CO_SCI_UI_PORT", "8080"))
        print(f"[ui] Server ready at http://127.0.0.1:{_port}")
        print(_format_notebook_runtime_startup_line())
        expected_venv_python = _project_venv_python()
        if expected_venv_python.exists():
            try:
                if Path(sys.executable).resolve() != expected_venv_python.resolve():
                    print(
                        "[ui] Startup warning: server is not using the project virtualenv. "
                        f"current={sys.executable} expected={expected_venv_python}"
                    )
            except Exception:
                logger.debug("Failed to print project virtualenv startup warning.", exc_info=True)
    else:
        print(f"[ui] Startup warning: {runtime.ready_error}")


@app.on_event("shutdown")
async def _shutdown() -> None:
    await runtime.shutdown()


@app.get("/")
async def index() -> HTMLResponse:
    return _render_ui_page("index.html")


@app.get("/about")
async def about() -> HTMLResponse:
    return _render_ui_page("about.html")


@app.get("/api/health")
async def health() -> dict:
    return {
        "ok": runtime.ready,
        "busy": any(lock.locked() for lock in runtime.conv_thread_locks.values()),
        "error": runtime.ready_error,
    }


@app.get("/api/rate-limit")
async def rate_limit_status(request: Request) -> dict:
    ip = _client_ip(request)
    remaining = query_limiter.remaining(ip)
    return {
        "limit": RATE_LIMIT_QUERIES,
        "remaining": remaining,
        "window_seconds": RATE_LIMIT_WINDOW,
    }


@app.get("/api/tasks")
async def list_tasks(request: Request) -> dict:
    ip = _client_ip(request)
    convs = runtime.list_conversations(owner_ip=ip)
    all_tasks = []
    for c in convs:
        tasks = runtime.store.get_conversation_tasks(c["conversation_id"])
        all_tasks.extend([_task_summary(t) for t in tasks])
    all_tasks.sort(key=lambda t: t.get("updated_at", ""), reverse=True)
    return {"tasks": all_tasks}


@app.get("/api/conversations")
async def list_conversations(request: Request) -> dict:
    ip = _client_ip(request)
    return {"conversations": runtime.list_conversations(owner_ip=ip)}


@app.get("/api/conversations/{conversation_id}")
async def conversation_detail(conversation_id: str, request: Request) -> dict:
    ip = _client_ip(request)
    if not runtime.store.conversation_owned_by(conversation_id, ip):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    detail = await runtime.get_conversation_detail(conversation_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    return detail


@app.get("/api/conversations/{conversation_id}/analysis-workspace")
async def conversation_analysis_workspace(conversation_id: str, request: Request) -> dict:
    ip = _client_ip(request)
    if not runtime.store.conversation_owned_by(conversation_id, ip):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    detail = await runtime.get_conversation_analysis_workspace(conversation_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    return detail


@app.get("/api/conversations/{conversation_id}/analysis-notebook")
async def conversation_analysis_notebook(conversation_id: str, request: Request) -> dict:
    ip = _client_ip(request)
    if not runtime.store.conversation_owned_by(conversation_id, ip):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    detail = await runtime.get_conversation_analysis_notebook(conversation_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    return detail


@app.get("/api/conversations/{conversation_id}/analysis-notebook.ipynb")
async def conversation_analysis_notebook_download(conversation_id: str, request: Request) -> Response:
    ip = _client_ip(request)
    if not runtime.store.conversation_owned_by(conversation_id, ip):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    detail = await runtime.get_conversation_analysis_notebook(conversation_id)
    if not detail or not isinstance(detail.get("notebook"), dict):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    ipynb_text = serialize_notebook_ipynb({"notebook": detail["notebook"]})
    filename = f"{conversation_id}-analysis.ipynb"
    return Response(
        content=ipynb_text,
        media_type="application/x-ipynb+json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/conversations/{conversation_id}/analysis-selection")
async def conversation_analysis_selection(
    conversation_id: str,
    request: Request,
    payload: AnalysisSelectionRequest,
) -> dict:
    ip = _client_ip(request)
    if not runtime.store.conversation_owned_by(conversation_id, ip):
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    try:
        detail = await runtime.set_conversation_selected_dataset(
            conversation_id,
            payload.dataset_id.strip(),
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Dataset not found in this analysis workspace.") from None
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not detail:
        raise HTTPException(status_code=404, detail=f"Conversation {conversation_id} not found.")
    return detail


@app.get("/api/tasks/{task_id}")
async def task_detail(task_id: str, request: Request) -> dict:
    task = runtime.store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    ip = _client_ip(request)
    conv_id = task.get("conversation_id", "")
    if conv_id and not runtime.store.conversation_owned_by(conv_id, ip):
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    detail = await runtime.get_task_detail(task_id)
    if not detail:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    return detail


@app.get("/api/tasks/{task_id}/debug/workflow-state")
async def task_workflow_state_debug(task_id: str, request: Request) -> dict:
    _check_task_ownership(task_id.strip(), request)
    detail = await runtime.get_task_workflow_state_debug(task_id.strip())
    if not detail:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    return detail


@app.get("/api/tasks/{task_id}/evidence-graph")
async def task_evidence_graph(task_id: str, request: Request) -> dict:
    _check_task_ownership(task_id.strip(), request)
    detail = await runtime.get_task_evidence_graph(task_id.strip())
    if not detail:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    return detail


@app.get("/api/tasks/{task_id}/dataset-visualizations")
async def task_dataset_visualizations(task_id: str, request: Request) -> dict:
    _check_task_ownership(task_id.strip(), request)
    detail = await runtime.get_task_dataset_visualizations(task_id.strip())
    if not detail:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    return detail


@app.post("/api/query")
async def start_query(payload: QueryRequest, request: Request) -> dict:
    if not runtime.ready:
        raise HTTPException(status_code=503, detail=runtime.ready_error or "Runtime not ready.")
    _enforce_rate_limit(request)
    ip = _client_ip(request)
    run = await runtime.start_new_query(
        payload.query.strip(),
        conversation_id=payload.conversation_id.strip() if payload.conversation_id else None,
        parent_task_id=payload.parent_task_id.strip() if payload.parent_task_id else None,
        mode=_normalize_task_mode(payload.mode),
        owner_ip=ip,
    )
    return run.to_dict()


def _check_task_ownership(task_id: str, request: Request) -> dict:
    """Return the task dict if it exists and belongs to the caller, else 404."""
    task = runtime.store.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    conv_id = task.get("conversation_id", "")
    ip = _client_ip(request)
    if conv_id and not runtime.store.conversation_owned_by(conv_id, ip):
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    return task


@app.post("/api/tasks/{task_id}/start")
async def start_task(task_id: str, request: Request, payload: StartRequest | None = None) -> dict:
    if not runtime.ready:
        raise HTTPException(status_code=503, detail=runtime.ready_error or "Runtime not ready.")
    _enforce_rate_limit(request)
    _check_task_ownership(task_id.strip(), request)
    run = await runtime.start_task(task_id.strip())
    return run.to_dict()


@app.post("/api/tasks/{task_id}/continue")
async def continue_task(task_id: str, request: Request) -> dict:
    if not runtime.ready:
        raise HTTPException(status_code=503, detail=runtime.ready_error or "Runtime not ready.")
    _enforce_rate_limit(request)
    _check_task_ownership(task_id.strip(), request)
    run = await runtime.start_task(task_id.strip())
    return run.to_dict()


@app.post("/api/tasks/{task_id}/feedback")
async def feedback_task(task_id: str, request: Request, payload: FeedbackRequest) -> dict:
    if not runtime.ready:
        raise HTTPException(status_code=503, detail=runtime.ready_error or "Runtime not ready.")
    _enforce_rate_limit(request)
    task = _check_task_ownership(task_id.strip(), request)
    msg = payload.message.strip()
    # When user types "approve"/"continue" etc. while plan is pending, treat as approval (start_task)
    # instead of revision feedback — otherwise "revise: approve" confuses the planner.
    if task.get("awaiting_hitl") and _is_continue_execution_command(msg):
        run = await runtime.start_task(task_id.strip())
        return run.to_dict()
    run = await runtime.feedback_task(task_id.strip(), msg)
    return run.to_dict()


@app.post("/api/tasks/{task_id}/revise")
async def revise_task(task_id: str, request: Request, payload: ReviseRequest) -> dict:
    if not runtime.ready:
        raise HTTPException(status_code=503, detail=runtime.ready_error or "Runtime not ready.")
    _enforce_rate_limit(request)
    _check_task_ownership(task_id.strip(), request)
    run = await runtime.feedback_task(task_id.strip(), payload.scope.strip())
    return run.to_dict()


@app.post("/api/tasks/{task_id}/rollback")
async def rollback_task(task_id: str, payload: RollbackRequest) -> dict:
    raise HTTPException(status_code=501, detail="Rollback not supported in this version.")


@app.get("/api/runs/{run_id}")
async def get_run(run_id: str) -> dict:
    payload = await runtime.get_run(run_id.strip())
    if not payload:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
    return payload


@app.get("/api/tasks/{task_id}/report.pdf")
async def export_report_pdf(task_id: str, request: Request) -> FileResponse:
    _check_task_ownership(task_id.strip(), request)
    task = runtime.store.get_task(task_id.strip())
    if not task:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found.")
    markdown = task.get("report_markdown", "").strip()
    if not markdown:
        raise HTTPException(status_code=404, detail="No report available for this task.")
    safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", task_id.strip())
    pdf_path = runtime._report_dir() / f"{safe_id}.pdf"
    error = write_markdown_pdf(markdown, pdf_path, title=task.get("title", "Report"))
    if error:
        raise HTTPException(status_code=503, detail=f"PDF export failed: {error}")
    return FileResponse(
        pdf_path,
        media_type="application/pdf",
        filename=f"{safe_id}.pdf",
    )


if __name__ == "__main__":
    import uvicorn

    host = os.environ.get("CO_SCI_UI_HOST", "127.0.0.1")
    port = int(os.environ.get("CO_SCI_UI_PORT", "8080"))
    uvicorn.run("ui_server:app", host=host, port=port, reload=False)
