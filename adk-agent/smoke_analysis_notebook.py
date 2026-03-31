#!/usr/bin/env python3
"""Run a live notebook smoke test against the local UI server."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any


DEFAULT_QUERY = "Find all schizphrenia datasets on openneuro and summarize their matedata by visuliaing it and plotting it"
TERMINAL_RUN_STATUSES = {"completed", "failed", "awaiting_hitl", "needs_clarification"}
TERMINAL_TASK_STATUSES = {"completed", "failed", "needs_clarification"}


def _request_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    body = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_request_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any] | None:
    try:
        return _request_json(method, url, payload)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return {"http_error": exc.code, "detail": detail}


def _extract_notebook_summary(payload: dict[str, Any]) -> dict[str, Any]:
    notebook = payload.get("notebook") if isinstance(payload.get("notebook"), dict) else {}
    cells = list(notebook.get("cells", []) or [])
    code_cells = [cell for cell in cells if str(cell.get("cell_type") or "").strip() == "code"]
    code_cell_count = len(code_cells)
    plot_output_count = 0
    error_output_count = 0
    diagnostic_markdown = ""
    for cell in cells:
        if str(cell.get("cell_type") or "").strip() == "markdown":
            source = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else str(cell.get("source") or "")
            if "Notebook diagnostics" in source:
                diagnostic_markdown = source
        for output in list(cell.get("outputs", []) or []):
            data = output.get("data") if isinstance(output.get("data"), dict) else {}
            if "image/svg+xml" in data or "image/png" in data:
                plot_output_count += 1
            if str(output.get("output_type") or "").strip() == "error":
                error_output_count += 1
    return {
        "cell_count": len(cells),
        "code_cell_count": code_cell_count,
        "plot_output_count": plot_output_count,
        "error_output_count": error_output_count,
        "diagnostics": diagnostic_markdown,
    }


def _progress_failures(task: dict[str, Any]) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for event in list(task.get("progress_events", []) or []):
        if not isinstance(event, dict):
            continue
        status = str(event.get("status") or "").strip().lower()
        event_type = str(event.get("event_type") or "").strip().lower()
        if status in {"error", "failed"} or event_type in {"tool.failed", "run.failed"}:
            failures.append(
                {
                    "event_type": event.get("event_type"),
                    "status": event.get("status"),
                    "human_line": event.get("human_line"),
                }
            )
    return failures


def run_smoke_test(base_url: str, query: str, *, timeout_s: int, poll_s: float) -> dict[str, Any]:
    started = _request_json(
        "POST",
        f"{base_url}/api/query",
        {"query": query, "mode": "analysis"},
    )
    initial_run_id = str(started.get("run_id") or "").strip()
    if not initial_run_id:
        raise RuntimeError(f"Missing run_id from /api/query response: {json.dumps(started, indent=2)}")
    deadline = time.time() + timeout_s
    current_run_id = initial_run_id
    current_run = started
    task_id = str(started.get("task_id") or "").strip()
    task: dict[str, Any] | None = None
    approval_runs: list[str] = []
    while time.time() < deadline:
        current_run = _request_json("GET", f"{base_url}/api/runs/{current_run_id}")
        task_id = task_id or str(current_run.get("task_id") or "").strip()
        if task_id:
            task_payload = _request_json("GET", f"{base_url}/api/tasks/{task_id}")
            task = task_payload.get("task") if isinstance(task_payload.get("task"), dict) else task_payload
            if bool(task.get("awaiting_hitl")):
                approval = _request_json("POST", f"{base_url}/api/tasks/{task_id}/start", {})
                approval_run_id = str(approval.get("run_id") or "").strip()
                if not approval_run_id:
                    raise RuntimeError(f"Missing run_id after approval: {json.dumps(approval, indent=2)}")
                approval_runs.append(approval_run_id)
                current_run_id = approval_run_id
                time.sleep(poll_s)
                continue
            if str(task.get("status") or "").strip() in TERMINAL_TASK_STATUSES:
                break
        if str(current_run.get("status") or "").strip() in TERMINAL_RUN_STATUSES and not task_id:
            break
        time.sleep(poll_s)

    if task_id and task is None:
        task_payload = _request_json("GET", f"{base_url}/api/tasks/{task_id}")
        task = task_payload.get("task") if isinstance(task_payload.get("task"), dict) else task_payload
    if not task_id:
        raise RuntimeError(f"Missing task_id after smoke run: {json.dumps(current_run, indent=2)}")
    if task is None:
        raise TimeoutError(f"Timed out waiting for task {task_id}.")

    conversation_id = str(task.get("conversation_id") or "").strip()
    if not conversation_id:
        raise RuntimeError(f"Missing conversation_id in task detail: {json.dumps(task, indent=2)}")
    notebook_payload = _request_json("GET", f"{base_url}/api/conversations/{conversation_id}/analysis-notebook")
    workflow_debug = _safe_request_json("GET", f"{base_url}/api/tasks/{task_id}/debug/workflow-state")
    summary = _extract_notebook_summary(notebook_payload)
    failures = _progress_failures(task)
    return {
        "query": query,
        "task_id": task_id,
        "conversation_id": conversation_id,
        "initial_run_id": initial_run_id,
        "approval_run_ids": approval_runs,
        "task_status": task.get("status"),
        "awaiting_hitl": task.get("awaiting_hitl"),
        "notebook_summary": summary,
        "progress_failures": failures,
        "task": task,
        "notebook_payload": notebook_payload,
        "workflow_debug": workflow_debug,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a live notebook smoke test against the local UI server.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--timeout-s", type=int, default=900)
    parser.add_argument("--poll-s", type=float, default=2.0)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    try:
        result = run_smoke_test(
            args.base_url.rstrip("/"),
            args.query,
            timeout_s=args.timeout_s,
            poll_s=args.poll_s,
        )
    except Exception as exc:  # pragma: no cover - live runner
        print(f"SMOKE TEST FAILED: {exc}", file=sys.stderr)
        return 1

    if args.output:
        with open(args.output, "w", encoding="utf-8") as handle:
            json.dump(result, handle, indent=2)

    summary = result["notebook_summary"]
    failures = result["progress_failures"]
    print(json.dumps(
        {
            "task_id": result["task_id"],
            "conversation_id": result["conversation_id"],
            "task_status": result["task_status"],
            "code_cell_count": summary["code_cell_count"],
            "plot_output_count": summary["plot_output_count"],
            "error_output_count": summary["error_output_count"],
            "progress_failure_count": len(failures),
            "diagnostics": summary["diagnostics"],
        },
        indent=2,
    ))

    if summary["code_cell_count"] < 1 or summary["plot_output_count"] < 1:
        print("Notebook smoke test failed: expected at least one code cell and one plot output.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
