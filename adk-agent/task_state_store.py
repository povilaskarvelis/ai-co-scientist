"""
Lightweight JSON-backed persistence for workflow tasks.
"""
from __future__ import annotations

import json
from pathlib import Path

from workflow import WorkflowTask


class TaskStateStore:
    """Persist workflow tasks to a local JSON file."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def _read_payload(self) -> dict:
        if not self.file_path.exists():
            return {"tasks": []}
        raw = self.file_path.read_text(encoding="utf-8").strip()
        if not raw:
            return {"tasks": []}
        try:
            payload = json.loads(raw)
            if "tasks" not in payload or not isinstance(payload["tasks"], list):
                return {"tasks": []}
            return payload
        except json.JSONDecodeError:
            # If the file is corrupted, preserve it but avoid crashing sessions.
            return {"tasks": []}

    def _write_payload(self, payload: dict) -> None:
        self.file_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def save_task(self, task: WorkflowTask) -> None:
        payload = self._read_payload()
        tasks = payload["tasks"]

        existing_index = next(
            (idx for idx, item in enumerate(tasks) if item.get("task_id") == task.task_id),
            None,
        )
        task_payload = task.to_dict()
        if existing_index is None:
            tasks.append(task_payload)
        else:
            tasks[existing_index] = task_payload
        self._write_payload(payload)

    def get_task(self, task_id: str) -> WorkflowTask | None:
        payload = self._read_payload()
        for item in payload["tasks"]:
            if item.get("task_id") == task_id:
                return WorkflowTask.from_dict(item)
        return None

    def list_tasks(self) -> list[WorkflowTask]:
        payload = self._read_payload()
        return [WorkflowTask.from_dict(item) for item in payload["tasks"]]

    def latest_task(self) -> WorkflowTask | None:
        tasks = self.list_tasks()
        if not tasks:
            return None
        tasks.sort(key=lambda task: task.updated_at, reverse=True)
        return tasks[0]
