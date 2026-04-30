from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


SCRIPT = Path(__file__).resolve().parent / "scripts" / "validate_investigation_artifacts.py"


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")


def append_jsonl(path: Path, *records: object) -> None:
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )


def make_investigation(root: Path, *, report_has_identifier: bool = True) -> Path:
    investigation = root / "lrrk2-parkinsons"
    investigation.mkdir()
    (investigation / "question.md").write_text(
        "Evaluate LRRK2 as a therapeutic target in Parkinson disease.\n",
        encoding="utf-8",
    )
    write_json(
        investigation / "plan.json",
        {
            "schema": "co_scientist.plan.v1",
            "objective": "Evaluate LRRK2 as a therapeutic target in Parkinson disease.",
            "status": "synthesized",
            "success_criteria": [
                "Summarize human genetic support.",
                "Capture clinical and druggability caveats.",
            ],
            "steps": [
                {
                    "id": "S1",
                    "goal": "Gather literature evidence for LRRK2 and Parkinson disease.",
                    "tool_hint": "search_pubmed",
                    "source_notes": "Targeted LRRK2/PD literature sweep.",
                    "domains": ["literature", "genomics"],
                    "completion_condition": "At least one PMID is recorded.",
                    "status": "completed",
                }
            ],
        },
    )
    append_jsonl(
        investigation / "evidence.jsonl",
        {
            "schema": "co_scientist.evidence.v1",
            "id": "E1",
            "step_id": "S1",
            "source": "PubMed",
            "tool": "search_pubmed",
            "query": "LRRK2 Parkinson disease",
            "claim": "LRRK2 variants are associated with Parkinson disease.",
            "summary": "PubMed evidence links LRRK2 to familial and sporadic Parkinson disease.",
            "identifiers": ["PMID:12345678"],
            "evidence_type": "literature",
            "confidence": "high",
            "limitations": [],
        },
    )
    append_jsonl(
        investigation / "claims.jsonl",
        {
            "schema": "co_scientist.claim.v1",
            "id": "C1",
            "claim": "LRRK2 has human genetic support in Parkinson disease.",
            "supporting_evidence": ["E1"],
            "source_identifiers": ["PMID:12345678"],
            "confidence": "high",
            "status": "supported",
        },
    )
    citation = "PMID:12345678" if report_has_identifier else "the cited literature"
    (investigation / "report.md").write_text(
        f"## TLDR\n\nLRRK2 has human genetic support in Parkinson disease ({citation}).\n",
        encoding="utf-8",
    )
    (investigation / "run_notes.md").write_text(
        "Temporary fixture for artifact validation tests.\n",
        encoding="utf-8",
    )
    return investigation


def run_validator(path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), str(path), *args],
        check=False,
        text=True,
        capture_output=True,
    )


def test_validate_report_stage_passes_for_complete_artifacts(tmp_path: Path) -> None:
    investigation = make_investigation(tmp_path)

    result = run_validator(investigation)

    assert result.returncode == 0, result.stderr
    assert "passed report validation" in result.stdout


def test_validate_plan_stage_allows_only_question_and_plan(tmp_path: Path) -> None:
    investigation = make_investigation(tmp_path)
    for filename in ("evidence.jsonl", "claims.jsonl", "report.md", "run_notes.md"):
        (investigation / filename).unlink()

    result = run_validator(investigation, "--stage", "plan")

    assert result.returncode == 0, result.stderr
    assert "passed plan validation" in result.stdout


def test_validate_report_stage_rejects_report_without_identifier(tmp_path: Path) -> None:
    investigation = make_investigation(tmp_path, report_has_identifier=False)

    result = run_validator(investigation)

    assert result.returncode == 1
    assert "report.md" in result.stderr
    assert "source identifier" in result.stderr


def test_validate_report_stage_rejects_unfinished_plan_state(tmp_path: Path) -> None:
    investigation = make_investigation(tmp_path)
    plan_path = investigation / "plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["status"] = "executing"
    plan["steps"][0]["status"] = "pending"
    write_json(plan_path, plan)

    result = run_validator(investigation)

    assert result.returncode == 1
    assert "synthesized" in result.stderr or "unfinished steps" in result.stderr


def test_validate_plan_stage_rejects_prose_tool_hint(tmp_path: Path) -> None:
    investigation = make_investigation(tmp_path)
    plan_path = investigation / "plan.json"
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    plan["steps"][0]["tool_hint"] = "PubMed + curated genetics summaries"
    write_json(plan_path, plan)
    for filename in ("evidence.jsonl", "claims.jsonl", "report.md", "run_notes.md"):
        (investigation / filename).unlink()

    result = run_validator(investigation, "--stage", "plan")

    assert result.returncode == 1
    assert "tool_hint" in result.stderr
    assert "machine-actionable" in result.stderr
