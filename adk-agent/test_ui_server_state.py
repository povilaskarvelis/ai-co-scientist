import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading
from types import SimpleNamespace

import pytest

from fastapi.testclient import TestClient
from google.adk.sessions import InMemorySessionService

for _env_name in ("AI_CO_SCIENTIST_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL"):
    os.environ.pop(_env_name, None)

import co_scientist.workflow as workflow
import ui_server
from co_scientist.workflow import (
    STATE_EXECUTOR_ACTIVE_STEP_ID,
    STATE_EXECUTOR_LAST_ERROR,
    STATE_PLAN_PENDING_APPROVAL,
    STATE_PRIOR_RESEARCH,
    STATE_REACT_PARSE_RETRIES,
    STATE_WORKFLOW_TASK,
)
from state_store import JsonTaskStore


class DummyRunner:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs


@pytest.fixture
def runtime(tmp_path, monkeypatch):
    for env_name in ("AI_CO_SCIENTIST_POSTGRES_DSN", "POSTGRES_DSN", "DATABASE_URL"):
        monkeypatch.delenv(env_name, raising=False)
    monkeypatch.setattr(
        ui_server,
        "create_workflow_agent",
        lambda require_plan_approval=True: (object(), None),
    )
    monkeypatch.setattr(ui_server, "Runner", DummyRunner)

    runtime = ui_server.UiRuntime(tmp_path / "workflow_tasks.json")
    runtime.session_service = InMemorySessionService()
    runtime.ready = True
    runtime.ready_error = None
    return runtime


def _sample_graph_task_state() -> dict:
    plan = {
        "schema": workflow.PLAN_SCHEMA,
        "objective": "Assess ataxia rare-disease evidence",
        "success_criteria": ["Summarize phenotype and disease evidence"],
        "steps": [
            {
                "id": "S1",
                "goal": "Map phenotype associations",
                "tool_hint": "query_monarch_associations",
                "domains": ["genomics"],
                "completion_condition": "Return phenotype-linked disease associations",
            },
        ],
    }
    task_state = workflow._initialize_task_state_from_plan(
        plan,
        objective_text="Assess ataxia rare-disease evidence",
    )
    workflow._apply_step_execution_result_to_task_state(
        task_state,
        {
            "schema": workflow.STEP_RESULT_SCHEMA,
            "step_id": "S1",
            "status": "completed",
            "step_progress_note": "Collected phenotype-linked associations.",
            "result_summary": "Ataxia maps to established phenotype-driven disease associations.",
            "evidence_ids": ["HP:0001251", "ORPHA:100"],
            "open_gaps": [],
            "suggested_next_searches": [],
            "tools_called": ["query_monarch_associations"],
            "structured_observations": [
                {
                    "observation_type": "phenotype_association",
                    "subject": {"type": "phenotype", "label": "Ataxia", "id": "HP:0001251"},
                    "predicate": "associated_with",
                    "object": {"type": "disease", "label": "Ataxia-telangiectasia", "id": "ORPHA:100"},
                    "supporting_ids": ["HP:0001251", "ORPHA:100"],
                    "source_tool": "query_monarch_associations",
                    "confidence": "medium",
                    "qualifiers": {"mode": "phenotype_to_disease"},
                }
            ],
        },
    )
    return task_state


def test_extract_persistable_session_state_filters_transient_keys():
    payload = {
        STATE_WORKFLOW_TASK: {"objective": "Test"},
        STATE_PRIOR_RESEARCH: [{"objective": "Earlier"}],
        STATE_PLAN_PENDING_APPROVAL: True,
        "temp:executor_buffer": "ignore me",
    }

    extracted = ui_server._extract_persistable_session_state(payload)

    assert extracted == {
        STATE_WORKFLOW_TASK: {"objective": "Test"},
        STATE_PRIOR_RESEARCH: [{"objective": "Earlier"}],
        STATE_PLAN_PENDING_APPROVAL: True,
    }


def _request_for_owner(owner_id: str, *, accept: str = ""):
    headers = [(b"accept", accept.encode("ascii"))] if accept else []
    request = ui_server.Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": "/",
            "raw_path": b"/",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )
    request.state.owner_id = owner_id
    return request


def test_signed_browser_owner_cookie_rejects_tampering():
    owner_id = ui_server._new_owner_id()
    signed = ui_server._sign_owner_id(owner_id)

    assert ui_server._verify_owner_cookie(signed) == owner_id
    assert ui_server._verify_owner_cookie(f"{owner_id}.invalid") is None
    assert ui_server._verify_owner_cookie("not-a-session") is None


def test_browser_session_middleware_sets_signed_http_only_cookie():
    client = TestClient(ui_server.app)

    response = client.get("/")

    assert response.status_code == 200
    cookie_value = client.cookies.get(ui_server.SESSION_COOKIE_NAME)
    assert ui_server._verify_owner_cookie(cookie_value or "") is not None
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "SameSite=lax" in response.headers["set-cookie"]


def test_query_stream_keeps_request_open_until_run_completes(tmp_path, monkeypatch):
    test_runtime = ui_server.UiRuntime(tmp_path / "workflow_tasks.json")
    test_runtime.ready = True
    test_runtime.ready_error = None

    async def fake_run_new_query(
        run_id: str,
        query: str,
        *,
        conversation_id: str | None = None,
        parent_task_id: str | None = None,
        owner_ip: str = "",
    ) -> None:
        await test_runtime._update_run(run_id, status="running")
        await asyncio.sleep(0)
        await test_runtime._update_run(run_id, status="completed", final_report="Done")

    test_runtime._run_new_query = fake_run_new_query  # type: ignore[method-assign]
    monkeypatch.setattr(ui_server, "runtime", test_runtime)
    client = TestClient(ui_server.app)

    response = client.post(
        "/api/query",
        headers={"Accept": "application/x-ndjson"},
        json={"query": "Test streaming"},
    )

    assert response.status_code == 200
    snapshots = [json.loads(line) for line in response.text.splitlines() if line.strip()]
    assert snapshots[-1]["status"] == "completed"
    assert snapshots[-1]["final_report"] == "Done"
    assert "owner_id" not in snapshots[-1]

    json_response = client.post("/api/query", json={"query": "Test JSON client"})
    assert json_response.status_code == 200
    assert json_response.json()["status"] == "completed"


def test_json_store_does_not_transfer_existing_conversation_owner(tmp_path):
    store = JsonTaskStore(tmp_path / "workflow_tasks.json")
    first = ui_server._make_task("task_first", "First", "conv_shared")
    second = ui_server._make_task("task_second", "Second", "conv_shared")

    store.save_task(first, owner_ip="session_original_owner_1234567890")
    store.save_task(second, owner_ip="session_attacker_owner_1234567890")

    assert store.conversation_owned_by("conv_shared", "session_original_owner_1234567890")
    assert not store.conversation_owned_by("conv_shared", "session_attacker_owner_1234567890")


def test_parse_step_event_text_uses_latest_step_block():
    text = """
### S1 · `completed`

**Goal:** Find disease IDs

**Key Findings**

Retrieved identifiers.

---

### S2 · `completed`

**Goal:** Query target evidence

**Key Findings**

+Found strong evidence.

_Progress: 2/4 steps complete. Next: S3_
""".strip()

    parsed = ui_server._parse_step_event_text(text)

    assert parsed["step_id"] == "S2"
    assert parsed["status"] == "completed"
    assert parsed["goal"] == "Query target evidence"
    assert "strong evidence" in parsed["findings"]


def test_build_step_completed_event_metrics_ignores_non_step_text():
    metrics = ui_server._build_step_completed_event_metrics(
        "_Completed 1 of 4 steps. Next: **S2**. Send `finalize` for a partial summary._"
    )

    assert metrics is None


def test_extract_executor_retry_metrics_from_session_state():
    metrics = ui_server._extract_executor_retry_metrics(
        {
            STATE_EXECUTOR_ACTIVE_STEP_ID: "S1",
            STATE_REACT_PARSE_RETRIES: 1,
            STATE_EXECUTOR_LAST_ERROR: "JSON parse error: Unexpected token",
        }
    )

    assert metrics == {
        "step_id": "S1",
        "retry_count": 1,
        "error": "JSON parse error: Unexpected token",
    }


def test_extract_tool_error_metrics_from_function_response():
    class DummyFunctionResponse:
        name = "run_bigquery_select_query"
        response = {
            "error": True,
            "error_type": "ValueError",
            "message": "Tool 'run_bigquery_select_query' failed: bad SQL",
            "suggestion": "Try a simpler query.",
        }

    metrics = ui_server._extract_tool_error_metrics(DummyFunctionResponse())

    assert metrics == {
        "tool": "run_bigquery_select_query",
        "error_type": "ValueError",
        "message": "Tool 'run_bigquery_select_query' failed: bad SQL",
        "suggestion": "Try a simpler query.",
    }


def test_derive_run_error_message_strips_markdown_noise():
    message = ui_server._derive_run_error_message(
        "## Execution Error\n\nVertex AI quota or rate limit exhausted.\n\n`429 RESOURCE_EXHAUSTED`",
        "Fallback error",
    )

    assert message == "Execution Error Vertex AI quota or rate limit exhausted. 429 RESOURCE_EXHAUSTED"


def test_visible_event_text_ignores_thought_parts():
    text = ui_server._visible_event_text(
        [
            SimpleNamespace(text="Hidden reasoning", thought=True),
            SimpleNamespace(text="Visible answer", thought=False),
            SimpleNamespace(text=" more", thought=False),
        ]
    )

    assert text == "Visible answer more"


def test_terminal_workflow_error_detection_matches_rate_limit_message():
    assert ui_server._is_terminal_workflow_error_response(
        "## Rate Limited\n\nGoogle AI Studio rate limits have been hit, so this run can't continue right now."
    )


def test_transient_workflow_response_matches_current_retry_status_line():
    assert ui_server._is_transient_workflow_response(
        "_Rate limit hit from Google AI Studio — retry 1/5, waited 5s…_"
    )


def test_fire_and_forget_threadsafe_does_not_block_on_future_result(monkeypatch):
    class FakeFuture:
        def __init__(self) -> None:
            self.result_calls = []
            self.callback_count = 0

        def add_done_callback(self, callback) -> None:
            self.callback_count += 1

        def result(self, *args, **kwargs):
            self.result_calls.append((args, kwargs))
            return None

    future = FakeFuture()

    async def sample_coro():
        return None

    def fake_run_coroutine_threadsafe(coro, loop):
        coro.close()
        return future

    monkeypatch.setattr(ui_server.asyncio, "run_coroutine_threadsafe", fake_run_coroutine_threadsafe)

    ui_server._fire_and_forget_threadsafe(sample_coro(), object(), label="emit_step_summary:test")

    assert future.callback_count == 1
    assert future.result_calls == []


@pytest.mark.asyncio
async def test_get_or_create_session_rehydrates_persisted_state(runtime):
    runtime.store.save_workflow_session(
        "conv_rehydrate",
        task_id="task_rehydrate",
        state={
            STATE_WORKFLOW_TASK: {"objective": "Restore me", "steps": []},
            STATE_PRIOR_RESEARCH: [{"objective": "Previous iteration"}],
            STATE_PLAN_PENDING_APPROVAL: True,
            "temp:co_scientist_executor_buffer": "discard",
        },
    )

    cs = await runtime._get_or_create_session("conv_rehydrate")
    session = await runtime.session_service.get_session(
        app_name=cs.app_name,
        user_id=runtime.user_id,
        session_id=cs.session_id,
    )

    assert session is not None
    assert session.state[STATE_WORKFLOW_TASK]["objective"] == "Restore me"
    assert session.state[STATE_PRIOR_RESEARCH] == [{"objective": "Previous iteration"}]
    assert session.state[STATE_PLAN_PENDING_APPROVAL] is True
    assert "temp:co_scientist_executor_buffer" not in session.state


@pytest.mark.asyncio
async def test_save_task_with_progress_persists_live_session_snapshot(runtime):
    async def fake_persistable_state(conversation_id: str) -> dict:
        assert conversation_id == "conv_persist"
        return {
            STATE_WORKFLOW_TASK: {"objective": "Persist me", "steps": []},
            STATE_PRIOR_RESEARCH: [{"objective": "Prior report"}],
            STATE_PLAN_PENDING_APPROVAL: False,
        }

    runtime._read_persistable_session_state = fake_persistable_state  # type: ignore[method-assign]

    task = ui_server._make_task("task_persist", "Persist me", "conv_persist")
    await runtime._save_task_with_progress(task, merge_progress=False)

    stored_task = runtime.store.get_task("task_persist")
    snapshot = runtime.store.get_workflow_session("conv_persist")

    assert stored_task is not None
    assert snapshot is not None
    assert snapshot["task_id"] == "task_persist"
    assert snapshot["state"] == {
        STATE_WORKFLOW_TASK: {"objective": "Persist me", "steps": []},
        STATE_PRIOR_RESEARCH: [{"objective": "Prior report"}],
        STATE_PLAN_PENDING_APPROVAL: False,
    }

    debug_payload = await runtime.get_task_workflow_state_debug("task_persist")
    assert debug_payload is not None
    assert debug_payload["source"] == "live"
    assert debug_payload["state"][STATE_WORKFLOW_TASK]["objective"] == "Persist me"


@pytest.mark.asyncio
async def test_get_task_evidence_graph_prefers_live_state(runtime):
    task_state = _sample_graph_task_state()

    async def fake_persistable_state(conversation_id: str) -> dict:
        assert conversation_id == "conv_graph_live"
        return {
            STATE_WORKFLOW_TASK: task_state,
            STATE_PLAN_PENDING_APPROVAL: False,
        }

    runtime._read_persistable_session_state = fake_persistable_state  # type: ignore[method-assign]
    runtime.store.save_workflow_session(
        "conv_graph_live",
        task_id="task_graph_live",
        state={STATE_WORKFLOW_TASK: {"objective": "Persisted placeholder", "steps": []}},
    )
    task = ui_server._make_task("task_graph_live", "Assess ataxia rare-disease evidence", "conv_graph_live")
    runtime.store.save_task(task)

    payload = await runtime.get_task_evidence_graph("task_graph_live")

    assert payload is not None
    assert payload["source"] == "live"
    assert payload["mode"] == "semantic"
    assert payload["summary"]["edge_count"] == 1
    assert payload["elements"]["edges"][0]["data"]["predicate"] == "associated_with"


@pytest.mark.asyncio
async def test_get_task_evidence_graph_falls_back_to_persisted_state(runtime):
    task_state = _sample_graph_task_state()
    task = ui_server._make_task("task_graph_persisted", "Assess ataxia rare-disease evidence", "conv_graph_persisted")
    runtime.store.save_task(task)
    runtime.store.save_workflow_session(
        "conv_graph_persisted",
        task_id="task_graph_persisted",
        state={STATE_WORKFLOW_TASK: task_state},
    )

    payload = await runtime.get_task_evidence_graph("task_graph_persisted")

    assert payload is not None
    assert payload["source"] == "persisted"
    assert payload["mode"] == "semantic"
    assert payload["summary"]["node_count"] == 2
    assert payload["summary"]["edge_count"] == 1
    assert payload["elements"]["nodes"][0]["data"]["type"] in {"disease", "phenotype"}


def test_json_store_persists_runs_and_interrupts_incomplete_runs(tmp_path):
    store = JsonTaskStore(tmp_path / "workflow_tasks.json")
    store.save_run(
        {
            "run_id": "run_123",
            "kind": "new_query",
            "status": "running",
            "task_id": "task_123",
            "logs": [],
            "progress_events": [],
            "progress_summaries": [],
            "created_at": "2026-03-06T00:00:00+00:00",
            "updated_at": "2026-03-06T00:00:00+00:00",
        }
    )

    assert store.get_run("run_123")["status"] == "running"

    updated = store.mark_incomplete_runs_failed("Run interrupted because the server restarted.")

    assert updated == 1
    restored = store.get_run("run_123")
    assert restored is not None
    assert restored["status"] == "failed"
    assert restored["error"] == "Run interrupted because the server restarted."
    assert any(event.get("type") == "run.interrupted" for event in restored["progress_events"])


def test_json_store_serializes_concurrent_writes_without_losing_tasks(tmp_path):
    state_path = tmp_path / "workflow_tasks.json"
    store = JsonTaskStore(state_path)
    conversation_id = "conv_concurrent"
    task_count = 40

    def save_task(index: int) -> None:
        store.save_task(
            ui_server._make_task(
                f"task_{index}",
                f"Concurrent task {index}",
                conversation_id,
            ),
            owner_ip="session-owner",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(save_task, range(task_count)))

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert len(persisted["tasks"]) == task_count
    assert len(persisted["conversations"][conversation_id]["task_ids"]) == task_count

    restored = JsonTaskStore(state_path)
    assert len(restored.get_conversation_tasks(conversation_id)) == task_count


def test_json_store_atomic_save_preserves_existing_file_when_replace_fails(tmp_path, monkeypatch):
    state_path = tmp_path / "workflow_tasks.json"
    store = JsonTaskStore(state_path)
    store.save_task(ui_server._make_task("task_original", "Original", "conv_atomic"))
    original_contents = state_path.read_text(encoding="utf-8")

    def fail_replace(source, destination) -> None:
        raise OSError("simulated atomic replacement failure")

    monkeypatch.setattr("state_store.os.replace", fail_replace)

    with pytest.raises(OSError, match="simulated atomic replacement failure"):
        store.save_task(ui_server._make_task("task_new", "New", "conv_atomic"))

    assert state_path.read_text(encoding="utf-8") == original_contents
    assert not list(tmp_path.glob(f".{state_path.name}.*.tmp"))


@pytest.mark.asyncio
async def test_export_report_pdf_runs_renderer_off_the_request_loop(runtime, monkeypatch):
    owner_id = ui_server._new_owner_id()
    task = ui_server._make_task("task_pdf", "PDF report", "conv_pdf")
    task["report_markdown"] = "# Completed report"
    runtime.store.save_task(task, owner_ip=owner_id)
    monkeypatch.setattr(ui_server, "runtime", runtime)
    request_thread_id = threading.get_ident()
    renderer_thread_ids: list[int] = []

    def fake_write_pdf(markdown, output_path, *, title):
        renderer_thread_ids.append(threading.get_ident())
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"%PDF-1.4\ncomplete\n%%EOF\n")
        return None

    monkeypatch.setattr(ui_server, "write_markdown_pdf", fake_write_pdf)

    response = await ui_server.export_report_pdf(
        task["task_id"],
        _request_for_owner(owner_id),
    )

    assert response.media_type == "application/pdf"
    assert renderer_thread_ids
    assert renderer_thread_ids[0] != request_thread_id


@pytest.mark.asyncio
async def test_export_report_pdf_does_not_expose_renderer_details(runtime, monkeypatch):
    owner_id = ui_server._new_owner_id()
    task = ui_server._make_task("task_pdf_error", "PDF report", "conv_pdf_error")
    task["report_markdown"] = "# Completed report"
    runtime.store.save_task(task, owner_ip=owner_id)
    monkeypatch.setattr(ui_server, "runtime", runtime)
    monkeypatch.setattr(
        ui_server,
        "write_markdown_pdf",
        lambda *args, **kwargs: "private renderer path: /tmp/sensitive",
    )

    with pytest.raises(ui_server.HTTPException) as exc_info:
        await ui_server.export_report_pdf(
            task["task_id"],
            _request_for_owner(owner_id),
        )

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail == "PDF export failed. Please try again."


@pytest.mark.asyncio
async def test_run_new_query_marks_terminal_rate_limit_response_as_failed(runtime):
    rate_limit_text = (
        "## Rate Limited\n\n"
        "Google AI Studio rate limits have been hit, so this run can't continue right now.\n\n"
        "Please try again later.\n\n"
        "`429 RESOURCE_EXHAUSTED`"
    )

    async def fake_get_or_create_session(conversation_id: str):
        return SimpleNamespace(app_name="test-app", session_id=conversation_id)

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        return rate_limit_text, "research_workflow"

    async def fake_read_state(conversation_id: str):
        return None

    async def fake_plan_pending(conversation_id: str) -> bool:
        return False

    runtime._get_or_create_session = fake_get_or_create_session  # type: ignore[method-assign]
    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]
    runtime._read_workflow_state = fake_read_state  # type: ignore[method-assign]
    runtime._is_plan_pending_approval = fake_plan_pending  # type: ignore[method-assign]

    run = await runtime._create_run("new_query", query="Assess obesity mechanisms")
    await runtime._run_new_query(run.run_id, "Assess obesity mechanisms")

    payload = await runtime.get_run(run.run_id)
    assert payload is not None
    assert payload["status"] == "failed"
    assert "Rate Limited" in payload["error"]

    task = runtime.store.get_task(payload["task_id"])
    assert task is not None
    assert task["status"] == "failed"
    assert task["report_markdown"] == rate_limit_text


@pytest.mark.asyncio
async def test_run_start_task_stops_immediately_on_terminal_rate_limit(runtime):
    rate_limit_text = (
        "## Rate Limited\n\n"
        "Google AI Studio rate limits have been hit, so this run can't continue right now.\n\n"
        "Please try again later.\n\n"
        "`429 RESOURCE_EXHAUSTED`"
    )

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        return rate_limit_text, "research_workflow"

    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]

    task = ui_server._make_task("task_rate_limit", "Assess obesity mechanisms", "conv_rate_limit")
    task["awaiting_hitl"] = True
    runtime.store.save_task(task)

    run = await runtime._create_run("start_task", task_id=task["task_id"])
    await runtime._run_start_task(run.run_id, task["task_id"])

    payload = await runtime.get_run(run.run_id)
    assert payload is not None
    assert payload["status"] == "failed"
    assert "Rate Limited" in payload["error"]

    stored_task = runtime.store.get_task(task["task_id"])
    assert stored_task is not None
    assert stored_task["status"] == "failed"
    assert stored_task["report_markdown"] == rate_limit_text


@pytest.mark.asyncio
async def test_run_start_task_marks_loop_exhaustion_failed(runtime):
    calls = 0

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        nonlocal calls
        calls += 1
        return "Still working", "research_workflow"

    async def fake_read_state(conversation_id: str):
        return {"plan_status": "ready", "steps": []}

    async def fake_plan_pending(conversation_id: str) -> bool:
        return False

    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]
    runtime._read_workflow_state = fake_read_state  # type: ignore[method-assign]
    runtime._is_plan_pending_approval = fake_plan_pending  # type: ignore[method-assign]

    task = ui_server._make_task("task_loop_exhausted", "Assess evidence", "conv_loop")
    task["awaiting_hitl"] = True
    runtime.store.save_task(task)

    run = await runtime._create_run("start_task", task_id=task["task_id"])
    await runtime._run_start_task(run.run_id, task["task_id"])

    payload = await runtime.get_run(run.run_id)
    stored_task = runtime.store.get_task(task["task_id"])
    assert calls == 100
    assert payload is not None
    assert payload["status"] == "failed"
    assert "did not reach a terminal state" in payload["error"]
    assert stored_task is not None
    assert stored_task["status"] == "failed"
    assert stored_task["awaiting_hitl"] is False


@pytest.mark.asyncio
async def test_stream_run_keeps_updates_public_and_waits_for_completion(runtime):
    owner_id = ui_server._new_owner_id()
    run = await runtime._create_run("new_query", query="Test", owner_id=owner_id)

    async def finish_run():
        await runtime._update_run(run.run_id, status="running")
        await asyncio.sleep(0)
        await runtime._update_run(run.run_id, status="completed", final_report="Done")

    job = asyncio.create_task(finish_run())
    runtime._track_background_task(run.run_id, job)
    snapshots = [json.loads(chunk) async for chunk in runtime.stream_run(run.run_id) if chunk.strip()]

    assert snapshots
    assert snapshots[-1]["status"] == "completed"
    assert snapshots[-1]["final_report"] == "Done"
    assert all("owner_id" not in snapshot for snapshot in snapshots)


@pytest.mark.asyncio
async def test_run_polling_requires_matching_browser_owner(runtime, monkeypatch):
    owner_id = ui_server._new_owner_id()
    run = await runtime._create_run("new_query", query="Test", owner_id=owner_id)
    monkeypatch.setattr(ui_server, "runtime", runtime)

    allowed = await ui_server._check_run_ownership(
        run.run_id,
        _request_for_owner(owner_id),
    )
    assert allowed["run_id"] == run.run_id

    with pytest.raises(ui_server.HTTPException) as exc_info:
        await ui_server._check_run_ownership(
            run.run_id,
            _request_for_owner(ui_server._new_owner_id()),
        )
    assert exc_info.value.status_code == 404


@pytest.mark.asyncio
async def test_start_query_rejects_foreign_conversation_before_starting(runtime, monkeypatch):
    owner_id = ui_server._new_owner_id()
    foreign_owner_id = ui_server._new_owner_id()
    task = ui_server._make_task("task_owned", "Owned question", "conv_owned")
    runtime.store.save_task(task, owner_ip=owner_id)
    monkeypatch.setattr(ui_server, "runtime", runtime)

    with pytest.raises(ui_server.HTTPException) as exc_info:
        await ui_server.start_query(
            ui_server.QueryRequest(
                query="Attempted branch",
                conversation_id="conv_owned",
            ),
            _request_for_owner(foreign_owner_id),
        )

    assert exc_info.value.status_code == 404
    assert runtime.runs == {}


@pytest.mark.asyncio
async def test_get_run_falls_back_to_persisted_store(runtime):
    runtime.store.save_run(
        {
            "run_id": "run_saved",
            "kind": "feedback_task",
            "status": "failed",
            "task_id": "task_saved",
            "logs": [],
            "progress_events": [],
            "progress_summaries": [],
            "error": "Persisted failure",
            "created_at": "2026-03-06T00:00:00+00:00",
            "updated_at": "2026-03-06T00:00:00+00:00",
        }
    )

    payload = await runtime.get_run("run_saved")

    assert payload is not None
    assert payload["run_id"] == "run_saved"
    assert payload["status"] == "failed"
    assert payload["error"] == "Persisted failure"
