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


class DummyMcpResources:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


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


def _request_for_owner(
    owner_id: str,
    *,
    accept: str = "",
    forwarded_for: str = "",
    client_host: str = "127.0.0.1",
):
    headers = []
    if accept:
        headers.append((b"accept", accept.encode("ascii")))
    if forwarded_for:
        headers.append((b"x-forwarded-for", forwarded_for.encode("ascii")))
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
            "client": (client_host, 12345),
            "server": ("testserver", 80),
        }
    )
    request.state.owner_id = owner_id
    return request


def test_client_ip_ignores_forwarded_header_without_trusted_proxy(monkeypatch):
    monkeypatch.setattr(ui_server, "RATE_LIMIT_TRUSTED_PROXY_HOPS", 0)
    request = _request_for_owner(
        "session_test_owner_1234567890123456",
        forwarded_for="198.51.100.77, 203.0.113.9",
        client_host="192.0.2.10",
    )

    assert ui_server._client_ip(request) == "192.0.2.10"


def test_client_ip_ignores_attacker_supplied_forwarded_prefix(monkeypatch):
    monkeypatch.setattr(ui_server, "RATE_LIMIT_TRUSTED_PROXY_HOPS", 1)
    request = _request_for_owner(
        "session_test_owner_1234567890123456",
        forwarded_for="198.51.100.77, 198.51.100.78, 203.0.113.42, 35.191.0.1",
        client_host="169.254.1.1",
    )

    assert ui_server._client_ip(request) == "203.0.113.42"


def test_client_ip_rejects_malformed_trusted_proxy_suffix(monkeypatch):
    monkeypatch.setattr(ui_server, "RATE_LIMIT_TRUSTED_PROXY_HOPS", 1)
    request = _request_for_owner(
        "session_test_owner_1234567890123456",
        forwarded_for="198.51.100.77, not-an-ip",
        client_host="192.0.2.10",
    )

    assert ui_server._client_ip(request) == "192.0.2.10"


def test_rate_limiter_caps_unique_keys_and_discards_oldest(monkeypatch):
    now = 1000.0
    monkeypatch.setattr(ui_server.time, "time", lambda: now)
    limiter = ui_server.RateLimiter(2, 60, max_keys=2)

    assert limiter.check("client-a") == (True, 0)
    assert limiter.check("client-b") == (True, 0)
    assert limiter.check("client-c") == (True, 0)

    assert list(limiter._hits) == ["client-b", "client-c"]
    assert len(limiter._hits) == 2


def test_rate_limiter_prunes_expired_key_before_evicting_active_key(monkeypatch):
    now = 1000.0
    monkeypatch.setattr(ui_server.time, "time", lambda: now)
    limiter = ui_server.RateLimiter(2, 60, max_keys=2)
    limiter.check("expired")
    now = 1061.0
    limiter.check("active")
    limiter.check("new")

    assert list(limiter._hits) == ["active", "new"]


def test_signed_browser_owner_cookie_rejects_tampering():
    owner_id = ui_server._new_owner_id()
    signed = ui_server._sign_owner_id(owner_id)

    assert ui_server._verify_owner_cookie(signed) == owner_id
    assert ui_server._verify_owner_cookie(f"{owner_id}.invalid") is None
    assert ui_server._verify_owner_cookie("not-a-session") is None


def test_configured_browser_session_secret_rejects_short_value(tmp_path, monkeypatch):
    monkeypatch.setattr(ui_server, "_configured_session_secret", "too-short")

    with pytest.raises(RuntimeError, match="at least 32 characters"):
        ui_server._configure_session_signing_key(tmp_path)


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


def test_planner_failure_response_detection_matches_empty_parse_output():
    assert ui_server._is_planner_failure_response("## Planner Parse Error\n\nEmpty model output.")
    assert ui_server._is_planner_failure_response("(No response)")
    assert not ui_server._is_planner_failure_response("Please provide the exact HGVS variant.")


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
async def test_conversation_retention_evicts_lru_resources(runtime, monkeypatch):
    resources: list[DummyMcpResources] = []

    def create_agent(require_plan_approval=True):
        managed = DummyMcpResources()
        resources.append(managed)
        return object(), managed

    monkeypatch.setattr(ui_server, "create_workflow_agent", create_agent)
    runtime.max_retained_conversations = 2

    oldest = await runtime._acquire_conversation_session("conv_oldest")
    await runtime._release_conversation_session("conv_oldest")
    newer = await runtime._acquire_conversation_session("conv_newer")
    await runtime._release_conversation_session("conv_newer")
    oldest.last_used_at = 1.0
    newer.last_used_at = 2.0

    await runtime._acquire_conversation_session("conv_newest")
    await runtime._release_conversation_session("conv_newest")

    assert set(runtime.conv_sessions) == {"conv_newer", "conv_newest"}
    assert resources[0].close_calls == 1
    assert resources[1].close_calls == 0
    assert resources[2].close_calls == 0
    assert await runtime.session_service.get_session(
        app_name=oldest.app_name,
        user_id=runtime.user_id,
        session_id=oldest.session_id,
    ) is None
    assert oldest.app_name not in runtime.session_service.sessions


@pytest.mark.asyncio
async def test_active_conversation_is_not_evicted(runtime, monkeypatch):
    resources: list[DummyMcpResources] = []

    def create_agent(require_plan_approval=True):
        managed = DummyMcpResources()
        resources.append(managed)
        return object(), managed

    monkeypatch.setattr(ui_server, "create_workflow_agent", create_agent)
    runtime.max_retained_conversations = 1

    active = await runtime._acquire_conversation_session("conv_active")
    other = await runtime._acquire_conversation_session("conv_other")

    assert set(runtime.conv_sessions) == {"conv_active", "conv_other"}
    assert active.active_operations == 1
    assert other.active_operations == 1

    await runtime._release_conversation_session("conv_other")

    assert set(runtime.conv_sessions) == {"conv_active"}
    assert resources[0].close_calls == 0
    assert resources[1].close_calls == 1

    runtime.max_retained_conversations = 0
    await runtime._release_conversation_session("conv_active")
    assert runtime.conv_sessions == {}
    assert resources[0].close_calls == 1


@pytest.mark.asyncio
async def test_evicted_conversation_rehydrates_persisted_state(runtime, monkeypatch):
    resources: list[DummyMcpResources] = []

    def create_agent(require_plan_approval=True):
        managed = DummyMcpResources()
        resources.append(managed)
        return object(), managed

    monkeypatch.setattr(ui_server, "create_workflow_agent", create_agent)
    runtime.max_retained_conversations = 0

    first = await runtime._acquire_conversation_session("conv_reopen")
    first_session = await runtime.session_service.get_session(
        app_name=first.app_name,
        user_id=runtime.user_id,
        session_id=first.session_id,
    )
    assert first_session is not None
    live_state = runtime.session_service.sessions[first.app_name][runtime.user_id][first.session_id].state
    live_state[STATE_WORKFLOW_TASK] = {
        "objective": "Resume after eviction",
        "steps": [],
    }
    live_state[STATE_PLAN_PENDING_APPROVAL] = True
    live_state["temp:discard_me"] = "transient"

    await runtime._persist_conversation_state("conv_reopen", task_id="task_reopen")
    await runtime._release_conversation_session("conv_reopen")

    assert runtime.conv_sessions == {}
    assert resources[0].close_calls == 1

    reopened = await runtime._acquire_conversation_session("conv_reopen")
    reopened_session = await runtime.session_service.get_session(
        app_name=reopened.app_name,
        user_id=runtime.user_id,
        session_id=reopened.session_id,
    )

    assert reopened_session is not None
    assert reopened.session_id != first.session_id
    assert reopened_session.state[STATE_WORKFLOW_TASK]["objective"] == "Resume after eviction"
    assert reopened_session.state[STATE_PLAN_PENDING_APPROVAL] is True
    assert "temp:discard_me" not in reopened_session.state

    await runtime._release_conversation_session("conv_reopen")
    assert resources[1].close_calls == 1


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


def test_json_store_interrupt_reconciles_only_the_matching_active_task(tmp_path):
    store = JsonTaskStore(tmp_path / "workflow_tasks.json")
    task = ui_server._make_task("task_active", "Active task", "conv_active")
    task["active_run_id"] = "run_active"
    store.save_task(task)
    store.save_run(
        {
            "run_id": "run_active",
            "kind": "start_task",
            "status": "running",
            "task_id": task["task_id"],
            "logs": [],
            "progress_events": [],
            "progress_summaries": [],
            "created_at": "2026-03-06T00:00:00+00:00",
            "updated_at": "2026-03-06T00:00:00+00:00",
        }
    )

    store.mark_incomplete_runs_failed(
        "Run interrupted during shutdown.",
        reason_code="server_shutdown",
    )

    restored_task = store.get_task(task["task_id"])
    restored_run = store.get_run("run_active")
    assert restored_task is not None
    assert restored_task["status"] == "failed"
    assert restored_task["awaiting_hitl"] is False
    assert "active_run_id" not in restored_task
    assert restored_run is not None
    assert restored_run["progress_events"][-1]["metrics"]["reason"] == "server_shutdown"


def test_json_store_prunes_old_terminal_runs_but_keeps_executing_runs(tmp_path):
    store = JsonTaskStore(tmp_path / "workflow_tasks.json")
    for run_id, status in (
        ("run_old", "completed"),
        ("run_middle", "failed"),
        ("run_new", "completed"),
        ("run_active", "running"),
    ):
        store.save_run(
            {
                "run_id": run_id,
                "kind": "new_query",
                "status": status,
                "logs": [],
                "progress_events": [],
                "progress_summaries": [],
                "created_at": "2026-03-06T00:00:00+00:00",
                "updated_at": "2026-03-06T00:00:00+00:00",
            }
        )

    removed = store.prune_terminal_runs(2)

    assert removed == 1
    assert store.get_run("run_old") is None
    assert store.get_run("run_middle") is not None
    assert store.get_run("run_new") is not None
    assert store.get_run("run_active")["status"] == "running"


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
async def test_shutdown_persists_interruption_and_blocks_late_run_writes(runtime):
    task = ui_server._make_task("task_shutdown", "Shutdown safety", "conv_shutdown")
    run = await runtime._create_run("start_task", task_id=task["task_id"])
    await runtime._update_run(run.run_id, status="running")
    task["active_run_id"] = run.run_id
    runtime.store.save_task(task)

    cancelled = asyncio.Event()

    async def pending_job() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    job = asyncio.create_task(pending_job())
    runtime._track_background_task(run.run_id, job)
    await asyncio.sleep(0)

    await runtime.shutdown()

    assert cancelled.is_set()
    persisted_run = runtime.store.get_run(run.run_id)
    persisted_task = runtime.store.get_task(task["task_id"])
    assert persisted_run is not None
    assert persisted_run["status"] == "failed"
    assert persisted_run["progress_events"][-1]["type"] == "run.interrupted"
    assert persisted_run["progress_events"][-1]["metrics"]["reason"] == "server_shutdown"
    assert persisted_task is not None
    assert persisted_task["status"] == "failed"
    assert "active_run_id" not in persisted_task

    await runtime._update_run(run.run_id, status="completed", final_report="late result")
    await runtime._append_progress_event(
        run.run_id,
        phase="finalize",
        event_type="run.completed",
        status="done",
        human_line="Late completion",
    )
    assert runtime.store.get_run(run.run_id)["status"] == "failed"


@pytest.mark.asyncio
async def test_cancelled_workflow_turn_keeps_resources_until_worker_finishes(runtime):
    started = threading.Event()
    release = threading.Event()

    async def slow_turn(*args, **kwargs):
        started.set()
        while not release.is_set():
            await asyncio.sleep(0.01)
        return "done", "research_workflow"

    runtime._workflow_turn_inner = slow_turn  # type: ignore[method-assign]
    turn = asyncio.create_task(
        runtime._run_workflow_turn("conv_worker", "test", run_id="run_worker")
    )
    for _ in range(100):
        if started.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set()

    turn.cancel()
    with pytest.raises(asyncio.CancelledError):
        await turn
    assert runtime.conv_sessions["conv_worker"].active_operations == 1

    release.set()
    for _ in range(100):
        if runtime.conv_sessions["conv_worker"].active_operations == 0:
            break
        await asyncio.sleep(0.01)
    assert runtime.conv_sessions["conv_worker"].active_operations == 0
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_failed_workflow_turn_releases_conversation(runtime):
    async def failed_turn(*args, **kwargs):
        raise RuntimeError("turn failed")

    runtime._workflow_turn_inner = failed_turn  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="turn failed"):
        await runtime._run_workflow_turn("conv_failed", "test", run_id="run_failed")

    assert runtime.conv_sessions["conv_failed"].active_operations == 0
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_workflow_turns_and_mcp_cleanup_share_one_event_loop(runtime, monkeypatch):
    turn_loops: list[asyncio.AbstractEventLoop] = []

    class LoopAwareMcpResources(DummyMcpResources):
        def __init__(self) -> None:
            super().__init__()
            self.close_loop: asyncio.AbstractEventLoop | None = None

        async def close(self) -> None:
            self.close_loop = asyncio.get_running_loop()
            await super().close()

    resources = LoopAwareMcpResources()
    monkeypatch.setattr(
        ui_server,
        "create_workflow_agent",
        lambda require_plan_approval=True: (object(), resources),
    )

    async def record_turn_loop(*args, **kwargs):
        turn_loops.append(asyncio.get_running_loop())
        return "done", "research_workflow"

    runtime._workflow_turn_inner = record_turn_loop  # type: ignore[method-assign]

    await runtime._run_workflow_turn("conv_loop", "first", run_id="run_first")
    await runtime._run_workflow_turn("conv_loop", "second", run_id="run_second")
    runtime.max_retained_conversations = 0
    await runtime._release_conversation_session("conv_loop")

    assert len(turn_loops) == 2
    assert turn_loops[0] is turn_loops[1]
    assert resources.close_loop is turn_loops[0]
    assert resources.close_loop is not asyncio.get_running_loop()
    assert resources.close_calls == 1
    assert "conv_loop" not in runtime._workflow_conversation_locks
    await runtime.shutdown()


@pytest.mark.asyncio
async def test_shutdown_waits_for_turn_cleanup_before_closing_mcp(runtime, monkeypatch):
    lifecycle_events: list[tuple[str, asyncio.AbstractEventLoop]] = []
    started = threading.Event()

    class LifecycleMcpResources(DummyMcpResources):
        async def close(self) -> None:
            lifecycle_events.append(("mcp_closed", asyncio.get_running_loop()))
            await super().close()

    resources = LifecycleMcpResources()
    monkeypatch.setattr(
        ui_server,
        "create_workflow_agent",
        lambda require_plan_approval=True: (object(), resources),
    )

    async def pending_turn(*args, **kwargs):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0)
            lifecycle_events.append(("turn_cleaned", asyncio.get_running_loop()))

    runtime._workflow_turn_inner = pending_turn  # type: ignore[method-assign]
    turn = asyncio.create_task(
        runtime._run_workflow_turn("conv_shutdown", "test", run_id="run_shutdown")
    )
    for _ in range(100):
        if started.is_set():
            break
        await asyncio.sleep(0.01)
    assert started.is_set()

    await runtime.shutdown()

    with pytest.raises(asyncio.CancelledError):
        await turn
    assert [event for event, _loop in lifecycle_events] == [
        "turn_cleaned",
        "mcp_closed",
    ]
    assert lifecycle_events[0][1] is lifecycle_events[1][1]
    assert resources.close_calls == 1
    assert not runtime._workflow_loop.is_running


@pytest.mark.asyncio
async def test_runtime_bounds_terminal_runs_in_memory_and_store(runtime):
    runtime.max_retained_completed_runs = 2
    run_ids = []
    for index in range(3):
        run = await runtime._create_run("new_query", query=f"Question {index}")
        run_ids.append(run.run_id)
        await runtime._update_run(run.run_id, status="completed")

    assert run_ids[0] not in runtime.runs
    assert runtime.store.get_run(run_ids[0]) is None
    assert set(run_ids[1:]).issubset(runtime.runs)


def test_generated_report_retention_is_bounded_by_report_group(runtime, tmp_path):
    report_dir = tmp_path / "reports"
    runtime._report_dir = lambda: report_dir  # type: ignore[method-assign]
    runtime.max_retained_reports = 10

    for index in range(3):
        markdown_path = runtime._write_report(f"task_{index}", f"Report {index}")
        pdf_path = report_dir / f"task_{index}.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n%%EOF\n")
        timestamp = 1_700_000_000 + index
        os.utime(markdown_path, (timestamp, timestamp))
        os.utime(pdf_path, (timestamp, timestamp))

    runtime.max_retained_reports = 2
    removed = runtime._prune_report_files()

    assert removed == 2
    assert not (report_dir / "task_0.md").exists()
    assert not (report_dir / "task_0.pdf").exists()
    assert (report_dir / "task_1.md").exists()
    assert (report_dir / "task_2.pdf").exists()


@pytest.mark.asyncio
async def test_run_new_query_retries_planner_parse_error_instead_of_completing_it(runtime):
    calls = 0

    async def fake_acquire_conversation_session(conversation_id: str):
        return SimpleNamespace(app_name="test-app", session_id=conversation_id)

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        nonlocal calls
        calls += 1
        if calls == 1:
            return "## Planner Parse Error\n\nEmpty model output.", "research_workflow"
        return "## Research Plan\n\n1. Inspect labels.", "research_workflow"

    async def fake_read_state(conversation_id: str):
        if calls < 2:
            return None
        return {
            "objective": "Compare two therapies",
            "plan_status": "ready",
            "steps": [{
                "id": "S1",
                "goal": "Inspect labels",
                "status": "pending",
                "tool_hint": "get_dailymed_drug_label",
                "completion_condition": "Both labels inspected",
            }],
        }

    async def fake_plan_pending(conversation_id: str) -> bool:
        return calls >= 2

    runtime._acquire_conversation_session = fake_acquire_conversation_session  # type: ignore[method-assign]
    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]
    runtime._read_workflow_state = fake_read_state  # type: ignore[method-assign]
    runtime._is_plan_pending_approval = fake_plan_pending  # type: ignore[method-assign]

    run = await runtime._create_run("new_query", query="Compare two therapies")
    await runtime._run_new_query(run.run_id, "Compare two therapies")

    payload = await runtime.get_run(run.run_id)
    task = runtime.store.get_task(payload["task_id"])
    assert calls == 2
    assert payload["status"] == "awaiting_hitl"
    assert task["status"] == "in_progress"
    assert not task.get("is_direct_response", False)
    assert len(task["steps"]) == 1


@pytest.mark.asyncio
async def test_feedback_task_retries_planner_parse_error_and_preserves_checkpoint(runtime):
    task = ui_server._make_task(
        "task_feedback_retry",
        "Compare therapies",
        "conv_feedback_retry",
        title="Compare therapies",
        user_query="Compare therapies",
    )
    task["status"] = "in_progress"
    task["awaiting_hitl"] = True
    task["steps"] = [{"id": "S1", "title": "Old step", "status": "pending"}]
    runtime.store.save_task(task)

    calls = 0
    restore_calls = []
    old_workflow = {
        "objective": "Compare therapies",
        "plan_status": "ready",
        "steps": [{"id": "S1", "goal": "Old step", "status": "pending"}],
    }
    new_workflow = {
        "objective": "Compare therapies",
        "plan_status": "ready",
        "steps": [{
            "id": "S1",
            "goal": "Revised step",
            "status": "pending",
            "tool_hint": "search_pubmed",
            "completion_condition": "Evidence retrieved",
        }],
    }

    async def fake_acquire_conversation_session(conversation_id: str):
        return SimpleNamespace(app_name="test-app", session_id=conversation_id)

    async def fake_read_persistable(conversation_id: str):
        return {
            ui_server.STATE_WORKFLOW_TASK: old_workflow,
            ui_server.STATE_PLAN_PENDING_APPROVAL: True,
        }

    async def fake_restore(conversation_id: str, snapshot: dict | None):
        restore_calls.append(snapshot)

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        nonlocal calls
        calls += 1
        if calls == 1:
            return "## Planner Parse Error\n\nEmpty model output.", "research_workflow"
        return "## Revised Research Plan", "research_workflow"

    async def fake_read_state(conversation_id: str):
        return None if calls < 2 else new_workflow

    async def fake_plan_pending(conversation_id: str) -> bool:
        return calls >= 2

    runtime._acquire_conversation_session = fake_acquire_conversation_session  # type: ignore[method-assign]
    runtime._read_persistable_session_state = fake_read_persistable  # type: ignore[method-assign]
    runtime._restore_persistable_session_state = fake_restore  # type: ignore[method-assign]
    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]
    runtime._read_workflow_state = fake_read_state  # type: ignore[method-assign]
    runtime._is_plan_pending_approval = fake_plan_pending  # type: ignore[method-assign]

    run = await runtime._create_run("feedback_task", task_id=task["task_id"])
    await runtime._run_feedback_task(run.run_id, task["task_id"], "Use a shorter plan")

    payload = await runtime.get_run(run.run_id)
    saved = runtime.store.get_task(task["task_id"])
    assert calls == 2
    assert len(restore_calls) == 1
    assert payload["status"] == "awaiting_hitl"
    assert saved["awaiting_hitl"] is True
    assert saved["steps"][0]["title"] == "Revised step"


@pytest.mark.asyncio
async def test_run_new_query_marks_terminal_rate_limit_response_as_failed(runtime):
    rate_limit_text = (
        "## Rate Limited\n\n"
        "Google AI Studio rate limits have been hit, so this run can't continue right now.\n\n"
        "Please try again later.\n\n"
        "`429 RESOURCE_EXHAUSTED`"
    )

    async def fake_acquire_conversation_session(conversation_id: str):
        return SimpleNamespace(app_name="test-app", session_id=conversation_id)

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        return rate_limit_text, "research_workflow"

    async def fake_read_state(conversation_id: str):
        return None

    async def fake_plan_pending(conversation_id: str) -> bool:
        return False

    runtime._acquire_conversation_session = fake_acquire_conversation_session  # type: ignore[method-assign]
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
    task["steps"] = [
        {
            "id": "S1",
            "goal": "Compare human evidence across candidate mechanisms",
            "status": "pending",
        }
    ]
    runtime.store.save_task(task)

    run = await runtime._create_run("start_task", task_id=task["task_id"])
    await runtime._run_start_task(run.run_id, task["task_id"])

    payload = await runtime.get_run(run.run_id)
    assert payload is not None
    assert payload["status"] == "failed"
    assert "Rate Limited" in payload["error"]
    execution_event = next(
        event for event in payload["progress_events"]
        if event["type"] == "execution.running"
    )
    assert execution_event["human_line"].startswith("Starting S1 of 1:")
    assert execution_event["metrics"] == {
        "step_id": "S1",
        "step_number": 1,
        "steps_total": 1,
    }

    stored_task = runtime.store.get_task(task["task_id"])
    assert stored_task is not None
    assert stored_task["status"] == "failed"
    assert stored_task["report_markdown"] == rate_limit_text


@pytest.mark.asyncio
async def test_run_start_task_rehydrates_released_conversation_before_saving(runtime):
    conversation_id = "conv_resume_checkpoint"
    task = ui_server._make_task(
        "task_resume_checkpoint",
        "Resume persisted workflow",
        conversation_id,
    )
    task["awaiting_hitl"] = True
    runtime.store.save_task(task)
    runtime.store.save_workflow_session(
        conversation_id,
        task_id=task["task_id"],
        state={
            STATE_WORKFLOW_TASK: {
                "objective": "Persisted checkpoint objective",
                "steps": [],
            },
            STATE_PLAN_PENDING_APPROVAL: True,
        },
    )
    runtime.max_retained_conversations = 0
    observed_objectives: list[str] = []

    async def fake_turn(conversation_id: str, prompt: str, *, run_id: str):
        workflow_state = await runtime._read_workflow_state(conversation_id)
        observed_objectives.append(workflow_state["objective"])
        return (
            "## Rate Limited\n\nGoogle AI Studio rate limits have been hit.\n\n"
            "`429 RESOURCE_EXHAUSTED`",
            "research_workflow",
        )

    runtime._run_workflow_turn_filtered = fake_turn  # type: ignore[method-assign]

    run = await runtime._create_run("start_task", task_id=task["task_id"])
    await runtime._run_start_task(run.run_id, task["task_id"])

    assert observed_objectives == ["Persisted checkpoint objective"]
    assert conversation_id not in runtime.conv_sessions
    persisted = runtime.store.get_workflow_session(conversation_id)
    assert persisted is not None
    assert persisted["state"][STATE_WORKFLOW_TASK]["objective"] == "Persisted checkpoint objective"


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
