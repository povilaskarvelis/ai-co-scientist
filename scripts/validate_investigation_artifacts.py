#!/usr/bin/env python3
"""Validate repo-native AI Co-Scientist investigation artifacts.

The validator intentionally uses only the Python standard library so it can run
before a project virtualenv is installed. It checks the stable artifact contract
rather than trying to enforce every JSON Schema feature.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
import re
import sys
from typing import Any


PLAN_SCHEMA = "co_scientist.plan.v1"
EVIDENCE_SCHEMA = "co_scientist.evidence.v1"
CLAIM_SCHEMA = "co_scientist.claim.v1"

PLAN_STATUSES = {"planned", "executing", "synthesized", "blocked"}
STEP_STATUSES = {"pending", "in_progress", "completed", "blocked", "skipped"}
EVIDENCE_TYPES = {
    "literature",
    "clinical_trial",
    "structured_data",
    "database_record",
    "regulatory",
    "experimental",
    "metadata",
    "other",
}
CONFIDENCE_LABELS = {"high", "moderate", "low", "mixed", "unknown"}
CLAIM_STATUSES = {"supported", "mixed", "unsupported", "needs_review"}

STAGE_RANK = {"plan": 1, "evidence": 2, "report": 3}
REQUIRED_BY_STAGE = {
    "plan": ("question.md", "plan.json"),
    "evidence": ("question.md", "plan.json", "evidence.jsonl"),
    "report": (
        "question.md",
        "plan.json",
        "evidence.jsonl",
        "claims.jsonl",
        "report.md",
        "run_notes.md",
    ),
}

IDENTIFIER_PATTERNS = (
    re.compile(r"\bPMID:\d+\b", re.IGNORECASE),
    re.compile(r"\bPMC\d+\b", re.IGNORECASE),
    re.compile(r"\bDOI:10\.\S+", re.IGNORECASE),
    re.compile(r"\bNCT\d{8}\b", re.IGNORECASE),
    re.compile(r"\bOpenAlex:W\d+\b", re.IGNORECASE),
    re.compile(r"\bUniProt:[A-Z0-9]+(?:-[0-9]+)?\b", re.IGNORECASE),
    re.compile(r"\bPubChem:(?:CID:)?\d+\b", re.IGNORECASE),
    re.compile(r"\bCHEMBL\d+\b", re.IGNORECASE),
    re.compile(r"\bPDB:[A-Z0-9]{4}\b", re.IGNORECASE),
    re.compile(r"\brs\d+\b", re.IGNORECASE),
    re.compile(r"\bGCST\d+\b", re.IGNORECASE),
    re.compile(r"\bReactome:R-HSA-\d+\b", re.IGNORECASE),
    re.compile(r"\bENSG\d+(?:\.\d+)?\b", re.IGNORECASE),
)
MACHINE_ACTIONABLE_TOOL_HINT_RE = re.compile(r"^[A-Za-z0-9_./:+-]+$")


@dataclass
class ValidationResult:
    errors: list[str]
    warnings: list[str]
    plan_step_ids: set[str]
    evidence_ids: set[str]
    source_identifiers: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class PlanValidation:
    step_ids: set[str]
    step_statuses: dict[str, str]
    status: str


def is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def looks_like_source_identifier(value: Any) -> bool:
    text = str(value or "").strip()
    return any(pattern.search(text) for pattern in IDENTIFIER_PATTERNS)


def read_json(path: Path, errors: list[str]) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        errors.append(f"Missing required file: {path.name}")
        return None
    except json.JSONDecodeError as exc:
        errors.append(f"{path.name}: invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}")
        return None
    if not isinstance(value, dict):
        errors.append(f"{path.name}: expected a JSON object")
        return None
    return value


def read_jsonl(path: Path, errors: list[str]) -> list[tuple[int, dict[str, Any]]]:
    records: list[tuple[int, dict[str, Any]]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        errors.append(f"Missing required file: {path.name}")
        return records

    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(
                f"{path.name}:{line_number}: invalid JSON at column {exc.colno}: {exc.msg}"
            )
            continue
        if not isinstance(value, dict):
            errors.append(f"{path.name}:{line_number}: expected a JSON object")
            continue
        records.append((line_number, value))
    return records


def check_required_files(root: Path, stage: str, errors: list[str]) -> None:
    for relative in REQUIRED_BY_STAGE[stage]:
        path = root / relative
        if not path.exists():
            errors.append(f"Missing required file: {relative}")
            continue
        if not path.is_file():
            errors.append(f"Expected a file but found something else: {relative}")
            continue
        if relative in {"question.md", "plan.json", "report.md", "run_notes.md"}:
            if not path.read_text(encoding="utf-8").strip():
                errors.append(f"{relative}: file must not be empty")


def validate_plan(root: Path, errors: list[str], warnings: list[str]) -> PlanValidation:
    plan = read_json(root / "plan.json", errors)
    if plan is None:
        return PlanValidation(set(), {}, "")

    if plan.get("schema") != PLAN_SCHEMA:
        errors.append(f"plan.json: schema must be {PLAN_SCHEMA!r}")
    if not is_nonempty_string(plan.get("objective")):
        errors.append("plan.json: objective must be a non-empty string")

    status = str(plan.get("status", "") or "")
    if status is not None and status not in PLAN_STATUSES:
        errors.append(f"plan.json: status must be one of {sorted(PLAN_STATUSES)}")

    success_criteria = plan.get("success_criteria")
    if not isinstance(success_criteria, list) or not success_criteria:
        errors.append("plan.json: success_criteria must be a non-empty array")
    elif not all(is_nonempty_string(item) for item in success_criteria):
        errors.append("plan.json: every success_criteria item must be a non-empty string")

    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        errors.append("plan.json: steps must be a non-empty array")
        return PlanValidation(set(), {}, status)

    step_ids: list[str] = []
    step_statuses: dict[str, str] = {}
    for index, step in enumerate(steps, start=1):
        prefix = f"plan.json: steps[{index - 1}]"
        if not isinstance(step, dict):
            errors.append(f"{prefix}: expected an object")
            continue

        step_id = step.get("id")
        expected_id = f"S{index}"
        if not is_nonempty_string(step_id) or not re.fullmatch(r"S[0-9]+", str(step_id)):
            errors.append(f"{prefix}: id must look like S{index}")
        elif step_id != expected_id:
            errors.append(f"{prefix}: expected sequential id {expected_id}, got {step_id}")
        else:
            step_ids.append(str(step_id))

        for field in ("goal", "tool_hint", "completion_condition"):
            if not is_nonempty_string(step.get(field)):
                errors.append(f"{prefix}: {field} must be a non-empty string")
        tool_hint = str(step.get("tool_hint", "") or "").strip()
        if tool_hint and not MACHINE_ACTIONABLE_TOOL_HINT_RE.fullmatch(tool_hint):
            errors.append(
                f"{prefix}: tool_hint must be a stable machine-actionable identifier "
                "without spaces, such as search_pubmed or get_clinical_trial"
            )

        domains = step.get("domains")
        if not isinstance(domains, list) or not (1 <= len(domains) <= 3):
            errors.append(f"{prefix}: domains must contain 1-3 strings")
        elif not all(is_nonempty_string(item) for item in domains):
            errors.append(f"{prefix}: domains must contain only non-empty strings")

        step_status = step.get("status")
        if step_status not in STEP_STATUSES:
            errors.append(f"{prefix}: status must be one of {sorted(STEP_STATUSES)}")
        elif isinstance(step_id, str):
            step_statuses[str(step_id)] = str(step_status)

    if len(step_ids) != len(set(step_ids)):
        errors.append("plan.json: step ids must be unique")

    if len(steps) > 12:
        warnings.append("plan.json: more than 12 steps may be too much for the first agent-native pass")

    return PlanValidation(set(step_ids), step_statuses, status)


def validate_evidence(
    root: Path,
    step_ids: set[str],
    errors: list[str],
    warnings: list[str],
) -> tuple[set[str], list[str]]:
    rows = read_jsonl(root / "evidence.jsonl", errors)
    if not rows:
        errors.append("evidence.jsonl: expected at least one evidence record")
        return set(), []

    evidence_ids: list[str] = []
    source_identifiers: list[str] = []
    for index, (line_number, record) in enumerate(rows, start=1):
        prefix = f"evidence.jsonl:{line_number}"
        if record.get("schema") != EVIDENCE_SCHEMA:
            errors.append(f"{prefix}: schema must be {EVIDENCE_SCHEMA!r}")

        evidence_id = record.get("id")
        expected_id = f"E{index}"
        if not is_nonempty_string(evidence_id) or not re.fullmatch(r"E[0-9]+", str(evidence_id)):
            errors.append(f"{prefix}: id must look like E{index}")
        elif evidence_id != expected_id:
            errors.append(f"{prefix}: expected sequential id {expected_id}, got {evidence_id}")
        else:
            evidence_ids.append(str(evidence_id))

        step_id = record.get("step_id")
        if step_id not in step_ids:
            errors.append(f"{prefix}: step_id {step_id!r} does not match any plan step")

        for field in ("source", "tool", "claim", "summary"):
            if not is_nonempty_string(record.get(field)):
                errors.append(f"{prefix}: {field} must be a non-empty string")

        identifiers = record.get("identifiers")
        if not isinstance(identifiers, list):
            errors.append(f"{prefix}: identifiers must be an array")
        else:
            for identifier in identifiers:
                if not is_nonempty_string(identifier):
                    errors.append(f"{prefix}: identifiers must contain only non-empty strings")
                    continue
                source_identifiers.append(str(identifier).strip())

        evidence_type = record.get("evidence_type")
        if evidence_type not in EVIDENCE_TYPES:
            errors.append(f"{prefix}: evidence_type must be one of {sorted(EVIDENCE_TYPES)}")

        confidence = record.get("confidence")
        if confidence not in CONFIDENCE_LABELS:
            errors.append(f"{prefix}: confidence must be one of {sorted(CONFIDENCE_LABELS)}")

        limitations = record.get("limitations")
        if limitations is not None:
            if not isinstance(limitations, list) or not all(isinstance(item, str) for item in limitations):
                errors.append(f"{prefix}: limitations must be an array of strings")

    if len(evidence_ids) != len(set(evidence_ids)):
        errors.append("evidence.jsonl: evidence ids must be unique")

    if not any(looks_like_source_identifier(identifier) for identifier in source_identifiers):
        errors.append(
            "evidence.jsonl: expected at least one recognizable source identifier "
            "(for example PMID, DOI, NCT, UniProt, PubChem, CHEMBL, PDB, rsID, or GCST)"
        )

    if len(rows) < len(step_ids):
        warnings.append("evidence.jsonl: fewer evidence records than plan steps; confirm each completed step is represented")

    return set(evidence_ids), source_identifiers


def validate_plan_progress(
    stage: str,
    plan: PlanValidation,
    root: Path,
    errors: list[str],
    warnings: list[str],
) -> None:
    if not plan.step_statuses:
        return

    evidence_rows = read_jsonl(root / "evidence.jsonl", []) if STAGE_RANK[stage] >= STAGE_RANK["evidence"] else []
    evidence_step_ids = {
        str(record.get("step_id", "") or "").strip()
        for _, record in evidence_rows
        if is_nonempty_string(record.get("step_id"))
    }

    if STAGE_RANK[stage] >= STAGE_RANK["evidence"]:
        for step_id in sorted(evidence_step_ids):
            step_status = plan.step_statuses.get(step_id, "")
            if step_status in {"pending", "in_progress"}:
                errors.append(
                    f"plan.json: step {step_id} has evidence records but is still marked {step_status!r}"
                )

    if STAGE_RANK[stage] >= STAGE_RANK["report"]:
        if plan.status != "synthesized":
            errors.append("plan.json: status must be 'synthesized' once report-stage artifacts exist")
        unfinished = [
            step_id for step_id, step_status in sorted(plan.step_statuses.items())
            if step_status in {"pending", "in_progress"}
        ]
        if unfinished:
            errors.append(
                "plan.json: report-stage artifacts require every step to be completed, blocked, or skipped; "
                f"unfinished steps: {', '.join(unfinished)}"
            )
        if not evidence_step_ids:
            warnings.append("report-stage artifacts exist but no evidence-linked steps were detected")


def validate_claims(
    root: Path,
    evidence_ids: set[str],
    errors: list[str],
) -> list[str]:
    rows = read_jsonl(root / "claims.jsonl", errors)
    if not rows:
        errors.append("claims.jsonl: expected at least one claim record")
        return []

    claim_ids: list[str] = []
    source_identifiers: list[str] = []
    for index, (line_number, record) in enumerate(rows, start=1):
        prefix = f"claims.jsonl:{line_number}"
        if record.get("schema") != CLAIM_SCHEMA:
            errors.append(f"{prefix}: schema must be {CLAIM_SCHEMA!r}")

        claim_id = record.get("id")
        expected_id = f"C{index}"
        if not is_nonempty_string(claim_id) or not re.fullmatch(r"C[0-9]+", str(claim_id)):
            errors.append(f"{prefix}: id must look like C{index}")
        elif claim_id != expected_id:
            errors.append(f"{prefix}: expected sequential id {expected_id}, got {claim_id}")
        else:
            claim_ids.append(str(claim_id))

        if not is_nonempty_string(record.get("claim")):
            errors.append(f"{prefix}: claim must be a non-empty string")

        supporting = record.get("supporting_evidence")
        if not isinstance(supporting, list) or not supporting:
            errors.append(f"{prefix}: supporting_evidence must be a non-empty array")
        else:
            for evidence_id in supporting:
                if evidence_id not in evidence_ids:
                    errors.append(f"{prefix}: supporting_evidence references unknown evidence id {evidence_id!r}")

        identifiers = record.get("source_identifiers")
        if not isinstance(identifiers, list):
            errors.append(f"{prefix}: source_identifiers must be an array")
        else:
            for identifier in identifiers:
                if not is_nonempty_string(identifier):
                    errors.append(f"{prefix}: source_identifiers must contain only non-empty strings")
                    continue
                source_identifiers.append(str(identifier).strip())

        confidence = record.get("confidence")
        if confidence not in CONFIDENCE_LABELS:
            errors.append(f"{prefix}: confidence must be one of {sorted(CONFIDENCE_LABELS)}")

        status = record.get("status")
        if status not in CLAIM_STATUSES:
            errors.append(f"{prefix}: status must be one of {sorted(CLAIM_STATUSES)}")

    if len(claim_ids) != len(set(claim_ids)):
        errors.append("claims.jsonl: claim ids must be unique")

    if not any(looks_like_source_identifier(identifier) for identifier in source_identifiers):
        errors.append("claims.jsonl: expected at least one recognizable source identifier")

    return source_identifiers


def validate_report(root: Path, source_identifiers: list[str], errors: list[str]) -> None:
    report_text = (root / "report.md").read_text(encoding="utf-8")
    if not any(pattern.search(report_text) for pattern in IDENTIFIER_PATTERNS):
        errors.append("report.md: expected at least one recognizable source identifier in the report")
        return

    normalized_report = report_text.lower()
    known_identifiers = [
        identifier for identifier in source_identifiers
        if looks_like_source_identifier(identifier)
    ]
    if known_identifiers and not any(identifier.lower() in normalized_report for identifier in known_identifiers):
        errors.append("report.md: expected at least one report citation to match an evidence or claim identifier")


def validate(root: Path, stage: str) -> ValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if not root.exists():
        errors.append(f"Investigation directory does not exist: {root}")
        return ValidationResult(errors, warnings, set(), set(), [])
    if not root.is_dir():
        errors.append(f"Investigation path is not a directory: {root}")
        return ValidationResult(errors, warnings, set(), set(), [])

    check_required_files(root, stage, errors)
    if errors:
        return ValidationResult(errors, warnings, set(), set(), [])

    plan = validate_plan(root, errors, warnings)
    evidence_ids: set[str] = set()
    source_identifiers: list[str] = []

    if STAGE_RANK[stage] >= STAGE_RANK["evidence"]:
        evidence_ids, source_identifiers = validate_evidence(root, plan.step_ids, errors, warnings)
        validate_plan_progress(stage, plan, root, errors, warnings)
    elif STAGE_RANK[stage] >= STAGE_RANK["plan"]:
        validate_plan_progress(stage, plan, root, errors, warnings)

    if STAGE_RANK[stage] >= STAGE_RANK["report"]:
        claim_identifiers = validate_claims(root, evidence_ids, errors)
        source_identifiers.extend(claim_identifiers)
        validate_report(root, source_identifiers, errors)

    return ValidationResult(errors, warnings, plan.step_ids, evidence_ids, source_identifiers)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate AI Co-Scientist repo-native investigation artifacts."
    )
    parser.add_argument(
        "path",
        type=Path,
        help="Path to .co-scientist/investigations/<slug>",
    )
    parser.add_argument(
        "--stage",
        choices=sorted(STAGE_RANK),
        default="report",
        help="Validation stage. Defaults to report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    result = validate(args.path, args.stage)

    for warning in result.warnings:
        print(f"WARNING: {warning}", file=sys.stderr)

    if not result.ok:
        for error in result.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    print(
        "OK: "
        f"{args.path} passed {args.stage} validation "
        f"({len(result.plan_step_ids)} step(s), "
        f"{len(result.evidence_ids)} evidence record(s), "
        f"{len(result.source_identifiers)} source identifier(s))"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
