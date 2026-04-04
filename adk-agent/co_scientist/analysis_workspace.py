"""Helpers for the conversation-level analysis notebook workspace."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from typing import Any
from typing import Mapping
import uuid

from .dataset_visualization import build_dataset_visualization_bundle


ANALYSIS_WORKSPACE_SCHEMA = "analysis_workspace.v1"
ANALYSIS_CONTEXT_SCHEMA = "analysis_context.v1"
DATASET_CATALOG_SCHEMA = "dataset_catalog.v1"
METADATA_SNAPSHOT_SCHEMA = "metadata_snapshot.v1"
DATASET_METADATA_PROFILE_SCHEMA = "dataset_metadata_profile.v1"
ANALYSIS_NOTE_SCHEMA = "analysis_note.v1"
ANALYSIS_CODE_CELL_SCHEMA = "analysis_code_cell.v1"
_OPENNEURO_ACCESSION_RE = re.compile(r"\b(ds\d{6,})\b", re.IGNORECASE)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compact_text(value: Any, *, max_chars: int = 280) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _slugify(value: Any, *, fallback: str = "item", max_chars: int = 72) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    text = text[:max_chars].strip("-")
    return text or fallback


def _dedupe_strings(values: list[Any] | None, *, limit: int = 12, max_chars: int = 120) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values or []:
        text = _compact_text(value, max_chars=max_chars)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        output.append(text)
        if len(output) >= limit:
            break
    return output


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _first_nonempty(mapping: Mapping[str, Any], keys: list[str]) -> Any:
    for key in keys:
        value = mapping.get(key)
        if value in (None, "", [], {}):
            continue
        return value
    return None


def _normalize_count(value: Any) -> int | str | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if value.is_integer():
            return int(value)
        return _compact_text(value, max_chars=32)
    text = _compact_text(value, max_chars=48)
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    if digits.isdigit():
        try:
            return int(digits)
        except ValueError:
            return text
    return text


def _normalize_bool_text(value: Any) -> str:
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = _compact_text(value, max_chars=48)
    return text


def parse_json_object_text(value: Any, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON object text: {exc.msg}.") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must decode to a JSON object.")
    return dict(parsed)


def parse_json_array_text(value: Any, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{field_name} must be valid JSON array text: {exc.msg}.") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"{field_name} must decode to a JSON array.")
    return parsed


def canonical_source_key(source: Any) -> str:
    text = str(source or "").strip().lower()
    if "openneuro" in text:
        return "openneuro"
    if "dandi" in text:
        return "dandi"
    if "nemar" in text:
        return "nemar"
    if "brainlife" in text:
        return "brainlife"
    if "neurodesk" in text:
        return "neurodesk"
    return _slugify(text or "dataset", fallback="dataset", max_chars=32)


def _normalize_accession(source_key: str, accession: Any) -> str:
    text = re.sub(r"\s+", "", str(accession or "").strip())
    if not text:
        return ""
    if source_key == "dandi" and ":" in text:
        prefix, suffix = text.split(":", 1)
        if prefix.strip().lower() == "dandi":
            text = suffix.strip()
    return text


def _is_generic_accession(source_key: str, source: Any, accession: Any) -> bool:
    normalized = str(accession or "").strip().lower()
    if not normalized:
        return True
    source_text = str(source or "").strip().lower()
    source_tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", source_text)
        if token
    }
    source_tokens.update({source_key, source_text, "dataset"})
    return normalized in source_tokens


def _infer_openneuro_accession(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        match = _OPENNEURO_ACCESSION_RE.search(text)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _infer_modalities_from_text(*values: Any) -> list[str]:
    corpus = " ".join(str(value or "") for value in values if str(value or "").strip())
    normalized = corpus.lower()
    found: list[str] = []
    checks = [
        ("rsfMRI", ("rsfmri", "resting-state fmri", "resting state fmri")),
        ("fMRI", (" fmri", "functional mri", "functional magnetic resonance imaging")),
        ("EEG", ("eeg", "electroencephalography")),
        ("MEG", ("meg", "magnetoencephalography")),
        ("PET", (" pet", "positron emission tomography")),
        ("iEEG", ("ieeg", "intracranial eeg")),
        ("MRI", (" mri", "structural mri", "magnetic resonance imaging")),
    ]
    for label, tokens in checks:
        if any(token in normalized for token in tokens):
            found.append(label)
    return _dedupe_strings(found, limit=6, max_chars=24)


def _canonical_modality_label(value: Any) -> str:
    text = _compact_text(value, max_chars=32)
    normalized = text.strip().lower()
    mapping = {
        "eeg": "EEG",
        "meg": "MEG",
        "pet": "PET",
        "ieeg": "iEEG",
        "mri": "MRI",
        "fmri": "fMRI",
        "rsfmri": "rsfMRI",
    }
    return mapping.get(normalized, text)


def _is_generic_dataset_label(label: str, source: str) -> bool:
    normalized_label = str(label or "").strip().lower()
    normalized_source = str(source or "").strip().lower()
    if not normalized_label:
        return True
    if normalized_label in {normalized_source, canonical_source_key(source), "dataset"}:
        return True
    return False


def canonical_dataset_id(source: Any, accession: Any = "", label: Any = "") -> str:
    source_key = canonical_source_key(source)
    normalized_accession = _normalize_accession(source_key, accession)
    if normalized_accession:
        return f"{source_key}:{normalized_accession.lower()}"
    return f"{source_key}:{_slugify(label or 'dataset', fallback='dataset', max_chars=48)}"


def _artifact_id() -> str:
    return f"artifact_{uuid.uuid4().hex[:10]}"


def _cell_id() -> str:
    return f"cell_{uuid.uuid4().hex[:10]}"


def _operation_id() -> str:
    return f"op_{uuid.uuid4().hex[:10]}"


def ensure_analysis_workspace(
    workspace: dict[str, Any] | None,
    *,
    conversation_id: str,
    title: str = "Open Data Analysis",
) -> dict[str, Any]:
    base = dict(workspace or {})
    base["schema"] = ANALYSIS_WORKSPACE_SCHEMA
    base["conversation_id"] = str(conversation_id or base.get("conversation_id") or "").strip()
    base["mode"] = "analysis"
    base["title"] = _compact_text(base.get("title") or title, max_chars=120) or title
    try:
        base["revision"] = int(base.get("revision", 0) or 0)
    except (TypeError, ValueError):
        base["revision"] = 0
    selected_dataset_id = str(base.get("selected_dataset_id") or "").strip()
    base["selected_dataset_id"] = selected_dataset_id or None
    base["datasets"] = dict(base.get("datasets") or {})
    base["artifacts"] = dict(base.get("artifacts") or {})
    base["cells"] = list(base.get("cells") or [])
    base["operations"] = list(base.get("operations") or [])
    return base


def _bump_workspace_revision(workspace: dict[str, Any]) -> None:
    workspace["revision"] = int(workspace.get("revision", 0) or 0) + 1


def _ensure_operation(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    intent: str,
    target_dataset_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_task_id = str(task_id or "").strip()
    if normalized_task_id:
        for operation in reversed(workspace.get("operations", [])):
            if str(operation.get("task_id") or "").strip() == normalized_task_id:
                if intent and not str(operation.get("intent") or "").strip():
                    operation["intent"] = intent
                if target_dataset_ids:
                    operation["target_dataset_ids"] = _dedupe_strings(
                        list(operation.get("target_dataset_ids", []) or []) + list(target_dataset_ids),
                        limit=12,
                        max_chars=80,
                    )
                return operation

    operation = {
        "operation_id": _operation_id(),
        "task_id": normalized_task_id,
        "user_prompt": _compact_text(user_prompt, max_chars=800),
        "intent": str(intent or "dataset_search_compare").strip() or "dataset_search_compare",
        "status": "completed",
        "created_at": _utc_now(),
        "produced_cell_ids": [],
        "target_dataset_ids": _dedupe_strings(target_dataset_ids or [], limit=12, max_chars=80),
    }
    workspace["operations"].append(operation)
    _bump_workspace_revision(workspace)
    return operation


def _upsert_workspace_dataset(
    workspace: dict[str, Any],
    *,
    dataset_id: str,
    source: str,
    accession: str,
    label: str,
    tags: list[str] | None,
    artifact_type: str = "",
    artifact_id: str = "",
) -> None:
    datasets = workspace.setdefault("datasets", {})
    current = dict(datasets.get(dataset_id) or {})
    latest_artifact_ids = dict(current.get("latest_artifact_ids") or {})
    if artifact_type and artifact_id:
        latest_artifact_ids[str(artifact_type).strip()] = str(artifact_id).strip()
    datasets[dataset_id] = {
        "dataset_id": dataset_id,
        "source": _compact_text(source, max_chars=48),
        "accession": _compact_text(accession, max_chars=48),
        "label": _compact_text(label, max_chars=120) or dataset_id,
        "tags": _dedupe_strings((current.get("tags") or []) + list(tags or []), limit=12, max_chars=48),
        "latest_artifact_ids": latest_artifact_ids,
        "last_seen_at": _utc_now(),
    }


def _append_artifact_cell(
    workspace: dict[str, Any],
    *,
    task_id: str,
    operation: dict[str, Any],
    artifact_type: str,
    artifact_schema: str,
    title: str,
    summary: str,
    payload: dict[str, Any],
    cell_type: str,
    dataset_id: str | None = None,
    evidence_ids: list[str] | None = None,
    depends_on: list[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    artifact = {
        "artifact_id": _artifact_id(),
        "type": artifact_type,
        "schema": artifact_schema,
        "title": _compact_text(title, max_chars=160) or artifact_type.replace("_", " ").title(),
        "task_id": str(task_id or "").strip(),
        "operation_id": str(operation.get("operation_id") or "").strip(),
        "created_at": _utc_now(),
        "summary": _compact_text(summary, max_chars=280),
        "depends_on": _dedupe_strings(depends_on or [], limit=12, max_chars=64),
        "evidence_ids": _dedupe_strings(evidence_ids or [], limit=20, max_chars=64),
        "payload": payload,
    }
    workspace.setdefault("artifacts", {})[artifact["artifact_id"]] = artifact

    cell = {
        "cell_id": _cell_id(),
        "type": cell_type,
        "task_id": str(task_id or "").strip(),
        "operation_id": str(operation.get("operation_id") or "").strip(),
        "created_at": _utc_now(),
        "title": artifact["title"],
        "artifact_id": artifact["artifact_id"],
        "status": "completed",
        "selected_dataset_id": dataset_id if dataset_id else None,
    }
    workspace.setdefault("cells", []).append(cell)

    produced_cell_ids = list(operation.get("produced_cell_ids", []) or [])
    produced_cell_ids.append(cell["cell_id"])
    operation["produced_cell_ids"] = produced_cell_ids
    if dataset_id:
        target_dataset_ids = list(operation.get("target_dataset_ids", []) or [])
        target_dataset_ids.append(dataset_id)
        operation["target_dataset_ids"] = _dedupe_strings(target_dataset_ids, limit=12, max_chars=80)

    _bump_workspace_revision(workspace)
    return artifact, cell


def _normalize_dataset_catalog_entry(row: Mapping[str, Any]) -> dict[str, Any]:
    def _metric_lookup(metrics: Mapping[str, Any], key: str) -> Any:
        if key in metrics:
            return metrics.get(key)
        alt_key = key.replace("_", "-")
        if alt_key in metrics:
            return metrics.get(alt_key)
        alt_key = key.replace("-", "_")
        if alt_key in metrics:
            return metrics.get(alt_key)
        return None

    def _list_field(*keys: str, fallback: list[str] | None = None) -> list[str]:
        for key in keys:
            value = row.get(key)
            if value in (None, "", [], {}):
                continue
            if isinstance(value, str):
                parts = re.split(r"[;,|]", value)
                return _dedupe_strings(parts, limit=8, max_chars=80)
            return _dedupe_strings(_as_list(value), limit=8, max_chars=80)
        return list(fallback or [])

    def _bool_field(*keys: str) -> bool | None:
        for key in keys:
            value = row.get(key)
            if value in (None, "", [], {}):
                continue
            if isinstance(value, bool):
                return value
            text = str(value).strip().lower()
            if text in {"yes", "true", "public", "available", "1"}:
                return True
            if text in {"no", "false", "restricted", "private", "0"}:
                return False
        return None

    source = _compact_text(row.get("source"), max_chars=48)
    source_key = canonical_source_key(source)
    raw_accession = _compact_text(
        row.get("accession") or row.get("dataset_accession") or row.get("id"),
        max_chars=48,
    )
    metrics = _as_mapping(row.get("metrics"))
    evidence_ids = _dedupe_strings(_as_list(row.get("evidence_ids")), limit=10, max_chars=64)
    tags = _dedupe_strings(_as_list(row.get("tags")), limit=10, max_chars=40)

    title = _compact_text(
        row.get("title") or row.get("dataset_title") or row.get("name"),
        max_chars=200,
    )
    description = _compact_text(
        row.get("description") or row.get("summary") or row.get("notes"),
        max_chars=320,
    )
    doi = _compact_text(row.get("doi"), max_chars=120)
    if not doi:
        doi = next(
            (
                str(item).split("DOI:", 1)[1].strip()
                for item in evidence_ids
                if str(item).upper().startswith("DOI:")
            ),
            "",
        )
    accession = _normalize_accession(source_key, raw_accession)
    if _is_generic_accession(source_key, source, accession):
        accession = ""
    if source_key == "openneuro":
        inferred_accession = _normalize_accession(
            source_key,
            _infer_openneuro_accession(
                accession,
                doi,
                title,
                row.get("dataset_label"),
                description,
                row.get("short_label"),
                row.get("id"),
            ),
        )
        if inferred_accession:
            accession = inferred_accession
        elif accession and not _OPENNEURO_ACCESSION_RE.fullmatch(accession):
            accession = ""

    label = _compact_text(row.get("dataset_label"), max_chars=120)
    if _is_generic_dataset_label(label, source):
        if title and not _is_generic_dataset_label(title, source):
            label = _compact_text(title, max_chars=120)
        elif accession:
            label = f"{source or source_key.title()} {accession}"
    label = label or accession or source or "Dataset"
    dataset_id = canonical_dataset_id(source, accession, label)

    participant_count = _first_nonempty(
        row,
        ["participant_count", "subject_count", "participant_n", "n_subjects", "n_participants"],
    )
    if participant_count in (None, "", [], {}):
        participant_count = _metric_lookup(metrics, "participant_count")
    if participant_count in (None, "", [], {}):
        participant_count = _metric_lookup(metrics, "subject_count")

    session_count = _first_nonempty(row, ["session_count", "sessions", "n_sessions"])
    if session_count in (None, "", [], {}):
        session_count = _metric_lookup(metrics, "session_count")

    title = title or label

    inferred_modalities = [
        tag
        for tag in tags
        if tag.upper() in {"MRI", "FMRI", "RSFMRI", "EEG", "MEG", "PET", "IEEG"}
        or tag.lower().startswith("modality:")
    ]
    inferred_modalities.extend(
        item
        for item in _infer_modalities_from_text(label, title, description)
        if item not in inferred_modalities
    )
    modalities = _dedupe_strings(
        [_canonical_modality_label(item) for item in _list_field("modalities", "modality", fallback=inferred_modalities)],
        limit=8,
        max_chars=24,
    )
    diagnoses = _list_field("diagnoses", "conditions", "condition")
    tasks = _list_field("tasks", "task")
    public_download = _bool_field("public_download", "is_public", "downloadable")
    if public_download is None:
        metric_public = _metric_lookup(metrics, "public_download")
        if isinstance(metric_public, bool):
            public_download = metric_public
    bids = _bool_field("bids", "is_bids")
    if bids is None:
        metric_bids = _metric_lookup(metrics, "bids")
        if isinstance(metric_bids, bool):
            bids = metric_bids
        elif any(str(tag).strip().lower() == "bids" for tag in tags):
            bids = True
    license_text = _compact_text(
        row.get("license") or _metric_lookup(metrics, "license"),
        max_chars=80,
    )
    return {
        "dataset_id": dataset_id,
        "dataset_label": label,
        "short_label": _compact_text(row.get("short_label") or label, max_chars=48) or label,
        "source": source,
        "accession": accession,
        "title": title,
        "description": description,
        "participant_count": _normalize_count(participant_count),
        "session_count": _normalize_count(session_count),
        "modalities": modalities,
        "diagnoses": diagnoses,
        "tasks": tasks,
        "public_download": public_download,
        "license": license_text,
        "doi": doi,
        "bids": bids,
        "overall_score": row.get("overall_score"),
        "notes": _compact_text(row.get("notes"), max_chars=240),
        "scores": dict(_as_mapping(row.get("scores"))),
        "metrics": {
            _slugify(key, fallback="metric", max_chars=32): value
            for key, value in metrics.items()
            if value not in (None, "")
        },
        "tags": tags,
        "evidence_ids": evidence_ids,
    }


def build_dataset_catalog_payload(
    *,
    objective: str,
    summary: str,
    dimensions: list[Any],
    datasets: list[Any],
    notes: list[str] | None = None,
) -> dict[str, Any]:
    normalized_datasets = [
        _normalize_dataset_catalog_entry(row)
        for row in list(datasets or [])
    ]
    normalized_dimensions: list[dict[str, Any]] = []
    for item in list(dimensions or []):
        if isinstance(item, Mapping):
            key = _slugify(item.get("key") or item.get("label"), fallback="dimension", max_chars=32).replace("-", "_")
            label = _compact_text(item.get("label") or item.get("key"), max_chars=80)
            if key and label:
                normalized_dimensions.append(
                    {
                        "key": key,
                        "label": label,
                        "description": _compact_text(item.get("description"), max_chars=160),
                        "weight": item.get("weight", 1),
                        "low_label": _compact_text(item.get("low_label"), max_chars=40),
                        "high_label": _compact_text(item.get("high_label"), max_chars=40),
                    }
                )
            continue
        label = _compact_text(item, max_chars=80)
        if label:
            normalized_dimensions.append(
                {
                    "key": _slugify(label, fallback="dimension", max_chars=32).replace("-", "_"),
                    "label": label,
                }
            )

    bundle: dict[str, Any] = {}
    if normalized_dimensions and any(_as_mapping(row).get("scores") for row in list(datasets or [])):
        try:
            bundle = build_dataset_visualization_bundle(
                objective=objective,
                summary=summary,
                dimensions=dimensions,
                datasets=datasets,
                notes=notes,
            )
        except ValueError:
            bundle = {}

    summary_cards = dict(bundle.get("summary_cards", {}) or {})
    if not summary_cards:
        source_count = len(
            {
                str(dataset.get("source") or "").strip()
                for dataset in normalized_datasets
                if str(dataset.get("source") or "").strip()
            }
        )
        known_participants = sum(1 for dataset in normalized_datasets if dataset.get("participant_count") not in (None, "", [], {}))
        summary_cards = {
            "dataset_count": len(normalized_datasets),
            "dimension_count": len(normalized_dimensions),
            "source_count": source_count,
            "datasets_with_participant_counts": known_participants,
        }

    if not bundle:
        bundle = {
            "objective": _compact_text(objective, max_chars=240),
            "summary": _compact_text(summary, max_chars=320),
            "rows": normalized_datasets,
            "dimensions": normalized_dimensions,
            "notes": _dedupe_strings(notes or [], limit=10, max_chars=120),
            "warnings": [],
            "summary_cards": summary_cards,
        }
    return {
        "schema": DATASET_CATALOG_SCHEMA,
        "generated_at": _utc_now(),
        "objective": _compact_text(objective, max_chars=240),
        "summary": _compact_text(summary, max_chars=320),
        "summary_cards": summary_cards,
        "dimensions": normalized_dimensions,
        "datasets": normalized_datasets,
        "visualizations": bundle,
        "notes": _dedupe_strings(notes or bundle.get("notes", []) or [], limit=10, max_chars=120),
        "warnings": _dedupe_strings(bundle.get("warnings", []) or [], limit=10, max_chars=160),
    }


def _normalize_linkish_list(values: Any) -> list[str]:
    output: list[str] = []
    for value in _as_list(values):
        if isinstance(value, Mapping):
            for candidate_key in ("url", "href", "link", "doi", "pmid", "id", "title", "citation"):
                candidate = value.get(candidate_key)
                text = _compact_text(candidate, max_chars=200)
                if text:
                    output.append(text)
                    break
            continue
        text = _compact_text(value, max_chars=200)
        if text:
            output.append(text)
    return _dedupe_strings(output, limit=12, max_chars=200)


def _coerce_section_mapping(value: Any, *, allow_lists: bool = False) -> dict[str, Any]:
    raw = _as_mapping(value)
    normalized: dict[str, Any] = {}
    for key, item in raw.items():
        label = _slugify(key, fallback="field", max_chars=40).replace("-", "_")
        if allow_lists and isinstance(item, (list, tuple)):
            items = _dedupe_strings(list(item), limit=12, max_chars=120)
            if items:
                normalized[label] = items
            continue
        if isinstance(item, bool):
            normalized[label] = _normalize_bool_text(item)
            continue
        if isinstance(item, (int, float)):
            normalized[label] = item
            continue
        text = _compact_text(item, max_chars=200)
        if text:
            normalized[label] = text
    return normalized


def _derive_identity_section(source_record: Mapping[str, Any], *, source: str, accession: str, label: str, dataset_id: str) -> dict[str, Any]:
    description = _first_nonempty(
        source_record,
        ["description", "summary", "study_description", "studyDescription", "abstract"],
    )
    doi = _first_nonempty(source_record, ["doi", "DOI"])
    version = _first_nonempty(source_record, ["version", "snapshotTag", "snapshot", "draft_version"])
    return {
        "dataset_id": dataset_id,
        "source": _compact_text(source, max_chars=48),
        "accession": _compact_text(accession, max_chars=48),
        "label": _compact_text(label, max_chars=160) or dataset_id,
        "description": _compact_text(description, max_chars=320),
        "doi": _compact_text(doi, max_chars=80),
        "version": _compact_text(version, max_chars=80),
    }


def _derive_access_section(source_record: Mapping[str, Any], *, source_key: str, accession: str) -> dict[str, Any]:
    landing_page = _first_nonempty(source_record, ["landing_page", "landingPage", "url", "datasetUrl", "link"])
    license_value = _first_nonempty(source_record, ["license", "license_name", "licenseName"])
    embargo = _first_nonempty(source_record, ["embargo", "embargo_status", "embargoStatus"])
    size_value = _first_nonempty(source_record, ["size", "size_gb", "sizeGb", "bytes", "asset_bytes"])
    asset_count = _first_nonempty(source_record, ["asset_count", "assetCount", "assets", "numberOfFiles"])
    public_value = _first_nonempty(source_record, ["public_download", "publicDownload", "is_public", "public"])
    access_value = _first_nonempty(source_record, ["access", "access_requirements", "download_access", "status"])
    if source_key == "openneuro" and accession and not landing_page:
        landing_page = f"https://openneuro.org/datasets/{accession}"
    if source_key == "dandi" and accession and not landing_page:
        landing_page = f"https://dandiarchive.org/dandiset/{accession}"
    return {
        "access": _compact_text(access_value, max_chars=120),
        "public_download": _normalize_bool_text(public_value) if public_value not in (None, "") else "",
        "license": _compact_text(license_value, max_chars=120),
        "embargo": _compact_text(embargo, max_chars=120),
        "landing_page": _compact_text(landing_page, max_chars=240),
        "size": _compact_text(size_value, max_chars=120),
        "asset_count": _normalize_count(asset_count),
    }


def _derive_subjects_section(source_record: Mapping[str, Any]) -> dict[str, Any]:
    subject_count = _first_nonempty(
        source_record,
        ["subject_count", "subjects", "participants", "participant_count", "sample_count", "number_of_subjects"],
    )
    session_count = _first_nonempty(source_record, ["session_count", "sessions", "number_of_sessions"])
    species = _first_nonempty(source_record, ["species", "organism", "subjectSpecies"])
    diagnosis = _first_nonempty(source_record, ["diagnosis", "condition", "disease", "diseases"])
    return {
        "subject_count": _normalize_count(subject_count),
        "session_count": _normalize_count(session_count),
        "species": _compact_text(species, max_chars=120),
        "condition_or_diagnosis": _compact_text(diagnosis, max_chars=180),
    }


def _collect_str_list(source_record: Mapping[str, Any], keys: list[str], *, limit: int = 12) -> list[str]:
    collected: list[Any] = []
    for key in keys:
        value = source_record.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, Mapping):
            for child_key in ("name", "label", "title", "id", "value"):
                child = value.get(child_key)
                if child not in (None, "", [], {}):
                    collected.append(child)
                    break
            continue
        collected.extend(_as_list(value))
    return _dedupe_strings(collected, limit=limit, max_chars=120)


def _derive_modalities_section(source_record: Mapping[str, Any], *, source_key: str) -> dict[str, Any]:
    modalities = _collect_str_list(
        source_record,
        ["modalities", "modality", "measurementTechnique", "techniques", "approach"],
    )
    tasks = _collect_str_list(source_record, ["tasks", "task", "paradigms", "paradigm", "task_name"])
    standards = _collect_str_list(source_record, ["standards", "standard", "schema", "schemas"])
    file_formats = _collect_str_list(source_record, ["file_formats", "fileFormats", "formats", "format"])
    if source_key == "openneuro" and "BIDS" not in standards:
        standards = _dedupe_strings(standards + ["BIDS"], limit=10, max_chars=48)
    if source_key == "dandi" and "NWB" not in file_formats:
        file_formats = _dedupe_strings(file_formats + ["NWB"], limit=10, max_chars=48)
    return {
        "modalities": modalities,
        "tasks": tasks,
        "standards": standards,
        "file_formats": file_formats,
    }


def _derive_links_publications_section(source_record: Mapping[str, Any]) -> dict[str, Any]:
    publications = _normalize_linkish_list(
        _first_nonempty(source_record, ["publications", "papers", "citations", "related_publications"])
    )
    links = _normalize_linkish_list(
        _first_nonempty(source_record, ["links", "related_links", "external_links", "resources"])
    )
    doi = _compact_text(_first_nonempty(source_record, ["doi", "DOI"]), max_chars=120)
    pmids = _dedupe_strings(
        _collect_str_list(source_record, ["pmids", "pubmed_ids", "pubmedIds"], limit=10),
        limit=10,
        max_chars=40,
    )
    if doi and doi not in publications:
        publications = _dedupe_strings([doi] + publications, limit=12, max_chars=200)
    for pmid in pmids:
        if pmid not in publications:
            publications.append(pmid)
    return {
        "publications": publications[:12],
        "links": links[:12],
    }


def _source_native_highlights(source_record: Mapping[str, Any], *, source_key: str) -> list[str]:
    highlights: list[str] = []
    for key in (
        "modalities",
        "tasks",
        "measurementTechnique",
        "species",
        "license",
        "status",
        "embargo",
        "snapshotTag",
        "version",
    ):
        value = source_record.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, (list, tuple)):
            rendered = ", ".join(_dedupe_strings(list(value), limit=4, max_chars=40))
        else:
            rendered = _compact_text(value, max_chars=120)
        if rendered:
            highlights.append(f"{key}: {rendered}")
    if source_key == "nemar":
        repo = _compact_text(_first_nonempty(source_record, ["repo", "repository", "repo_name"]), max_chars=80)
        if repo:
            highlights.append(f"repository: {repo}")
    return _dedupe_strings(highlights, limit=10, max_chars=160)


def _normalize_metadata_sections(
    source_record: Mapping[str, Any],
    *,
    source: str,
    accession: str,
    label: str,
    dataset_id: str,
) -> dict[str, Any]:
    source_key = canonical_source_key(source)
    return {
        "identity": _derive_identity_section(source_record, source=source, accession=accession, label=label, dataset_id=dataset_id),
        "access_download": _derive_access_section(source_record, source_key=source_key, accession=accession),
        "subjects_sessions": _derive_subjects_section(source_record),
        "modalities_tasks": _derive_modalities_section(source_record, source_key=source_key),
        "standards_file_formats": _coerce_section_mapping(
            {
                "standards": _collect_str_list(source_record, ["standards", "standard", "schema", "schemas"], limit=10),
                "file_formats": _collect_str_list(source_record, ["file_formats", "fileFormats", "formats", "format"], limit=10),
            },
            allow_lists=True,
        ),
        "links_publications": _derive_links_publications_section(source_record),
        "source_raw_highlights": _source_native_highlights(source_record, source_key=source_key),
    }


def normalize_dataset_metadata_profile(metadata: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(metadata or {})
    source = _compact_text(_first_nonempty(raw, ["source", "archive", "repository", "database"]), max_chars=48)
    accession = _compact_text(_first_nonempty(raw, ["accession", "dataset_id", "datasetId", "identifier", "id"]), max_chars=48)
    label = _compact_text(_first_nonempty(raw, ["dataset_label", "label", "title", "name"]), max_chars=160)
    dataset_id = canonical_dataset_id(source, accession, label)
    summary = _compact_text(_first_nonempty(raw, ["summary", "description", "overview"]), max_chars=320)
    evidence_ids = _dedupe_strings(_as_list(raw.get("evidence_ids")), limit=16, max_chars=64)

    has_explicit_sections = any(
        key in raw
        for key in (
            "identity",
            "access_download",
            "subjects_sessions",
            "modalities_tasks",
            "standards_file_formats",
            "links_publications",
            "source_raw_highlights",
        )
    )

    if has_explicit_sections:
        identity = _coerce_section_mapping(raw.get("identity"))
        identity.setdefault("dataset_id", dataset_id)
        identity.setdefault("source", source)
        identity.setdefault("accession", accession)
        identity.setdefault("label", label or dataset_id)
        sections = {
            "identity": identity,
            "access_download": _coerce_section_mapping(raw.get("access_download")),
            "subjects_sessions": _coerce_section_mapping(raw.get("subjects_sessions")),
            "modalities_tasks": _coerce_section_mapping(raw.get("modalities_tasks"), allow_lists=True),
            "standards_file_formats": _coerce_section_mapping(raw.get("standards_file_formats"), allow_lists=True),
            "links_publications": _coerce_section_mapping(raw.get("links_publications"), allow_lists=True),
            "source_raw_highlights": _dedupe_strings(_as_list(raw.get("source_raw_highlights")), limit=10, max_chars=160),
        }
    else:
        sections = _normalize_metadata_sections(
            raw,
            source=source,
            accession=accession,
            label=label or accession or source or "Dataset",
            dataset_id=dataset_id,
        )

    return {
        "schema": DATASET_METADATA_PROFILE_SCHEMA,
        "generated_at": _utc_now(),
        "dataset_id": dataset_id,
        "source": source,
        "accession": accession,
        "dataset_label": label or accession or source or dataset_id,
        "summary": summary,
        "evidence_ids": evidence_ids,
        "identity": sections["identity"],
        "access_download": sections["access_download"],
        "subjects_sessions": sections["subjects_sessions"],
        "modalities_tasks": sections["modalities_tasks"],
        "standards_file_formats": sections["standards_file_formats"],
        "links_publications": sections["links_publications"],
        "source_raw_highlights": sections["source_raw_highlights"],
    }


def append_dataset_catalog_artifact(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    objective: str,
    summary: str,
    dimensions: list[Any],
    datasets: list[Any],
    notes: list[str] | None = None,
) -> dict[str, Any]:
    payload = build_dataset_catalog_payload(
        objective=objective,
        summary=summary,
        dimensions=dimensions,
        datasets=datasets,
        notes=notes,
    )
    dataset_ids = [str(item.get("dataset_id") or "").strip() for item in payload.get("datasets", []) if str(item.get("dataset_id") or "").strip()]
    operation = _ensure_operation(
        workspace,
        task_id=task_id,
        user_prompt=user_prompt,
        intent="dataset_search_compare",
        target_dataset_ids=dataset_ids,
    )
    title = _compact_text(summary or objective or "Dataset comparison", max_chars=160) or "Dataset comparison"
    artifact, cell = _append_artifact_cell(
        workspace,
        task_id=task_id,
        operation=operation,
        artifact_type="dataset_catalog",
        artifact_schema=DATASET_CATALOG_SCHEMA,
        title=title,
        summary=summary or objective,
        payload=payload,
        cell_type="dataset_comparison",
    )
    for dataset in payload.get("datasets", []):
        dataset_id = str(dataset.get("dataset_id") or "").strip()
        if not dataset_id:
            continue
        _upsert_workspace_dataset(
            workspace,
            dataset_id=dataset_id,
            source=str(dataset.get("source") or "").strip(),
            accession=str(dataset.get("accession") or "").strip(),
            label=str(dataset.get("dataset_label") or "").strip(),
            tags=list(dataset.get("tags") or []),
            artifact_type="dataset_catalog",
            artifact_id=str(artifact.get("artifact_id") or "").strip(),
        )
    return {
        "operation_id": operation["operation_id"],
        "artifact_id": artifact["artifact_id"],
        "cell_id": cell["cell_id"],
        "payload": payload,
    }


def append_metadata_snapshot_artifact(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    objective: str,
    summary: str,
    dimensions: list[Any],
    datasets: list[Any],
    notes: list[str] | None = None,
) -> dict[str, Any]:
    payload = build_dataset_catalog_payload(
        objective=objective,
        summary=summary,
        dimensions=dimensions,
        datasets=datasets,
        notes=notes,
    )
    payload["schema"] = METADATA_SNAPSHOT_SCHEMA
    dataset_ids = [
        str(item.get("dataset_id") or "").strip()
        for item in payload.get("datasets", [])
        if str(item.get("dataset_id") or "").strip()
    ]
    operation = _ensure_operation(
        workspace,
        task_id=task_id,
        user_prompt=user_prompt,
        intent="dataset_search_compare",
        target_dataset_ids=dataset_ids,
    )
    title = _compact_text(summary or objective or "Metadata snapshot", max_chars=160) or "Metadata snapshot"
    artifact, cell = _append_artifact_cell(
        workspace,
        task_id=task_id,
        operation=operation,
        artifact_type="metadata_snapshot",
        artifact_schema=METADATA_SNAPSHOT_SCHEMA,
        title=title,
        summary=summary or objective,
        payload=payload,
        cell_type="metadata_snapshot",
    )
    for dataset in payload.get("datasets", []):
        dataset_id = str(dataset.get("dataset_id") or "").strip()
        if not dataset_id:
            continue
        _upsert_workspace_dataset(
            workspace,
            dataset_id=dataset_id,
            source=str(dataset.get("source") or "").strip(),
            accession=str(dataset.get("accession") or "").strip(),
            label=str(dataset.get("dataset_label") or "").strip(),
            tags=list(dataset.get("tags") or []),
            artifact_type="metadata_snapshot",
            artifact_id=str(artifact.get("artifact_id") or "").strip(),
        )
    return {
        "operation_id": operation["operation_id"],
        "artifact_id": artifact["artifact_id"],
        "cell_id": cell["cell_id"],
        "payload": payload,
    }


def append_dataset_metadata_artifact(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    metadata: Mapping[str, Any],
    title: str = "",
    summary: str = "",
) -> dict[str, Any]:
    payload = normalize_dataset_metadata_profile(metadata)
    dataset_id = str(payload.get("dataset_id") or "").strip()
    operation = _ensure_operation(
        workspace,
        task_id=task_id,
        user_prompt=user_prompt,
        intent="dataset_metadata",
        target_dataset_ids=[dataset_id] if dataset_id else [],
    )
    resolved_title = (
        _compact_text(title, max_chars=160)
        or f"Metadata profile · {_compact_text(payload.get('dataset_label'), max_chars=120)}"
    )
    artifact, cell = _append_artifact_cell(
        workspace,
        task_id=task_id,
        operation=operation,
        artifact_type="dataset_metadata",
        artifact_schema=DATASET_METADATA_PROFILE_SCHEMA,
        title=resolved_title,
        summary=summary or str(payload.get("summary") or "").strip(),
        payload=payload,
        cell_type="dataset_metadata",
        dataset_id=dataset_id or None,
        evidence_ids=list(payload.get("evidence_ids", []) or []),
    )
    _upsert_workspace_dataset(
        workspace,
        dataset_id=dataset_id,
        source=str(payload.get("source") or "").strip(),
        accession=str(payload.get("accession") or "").strip(),
        label=str(payload.get("dataset_label") or "").strip(),
        tags=_collect_str_list(payload, ["modalities_tasks"], limit=8),
        artifact_type="dataset_metadata",
        artifact_id=str(artifact.get("artifact_id") or "").strip(),
    )
    if dataset_id:
        workspace["selected_dataset_id"] = dataset_id
        _bump_workspace_revision(workspace)
    return {
        "operation_id": operation["operation_id"],
        "artifact_id": artifact["artifact_id"],
        "cell_id": cell["cell_id"],
        "payload": payload,
        "dataset_id": dataset_id,
    }


def append_analysis_note_artifact(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    markdown: str,
    title: str = "",
    related_dataset_ids: list[str] | None = None,
) -> dict[str, Any]:
    clean_markdown = str(markdown or "").strip()
    if not clean_markdown:
        raise ValueError("markdown must not be empty.")
    operation = _ensure_operation(
        workspace,
        task_id=task_id,
        user_prompt=user_prompt,
        intent="dataset_metadata" if related_dataset_ids else "dataset_search_compare",
        target_dataset_ids=related_dataset_ids,
    )
    payload = {
        "schema": ANALYSIS_NOTE_SCHEMA,
        "created_at": _utc_now(),
        "markdown": clean_markdown,
        "related_dataset_ids": _dedupe_strings(related_dataset_ids or [], limit=12, max_chars=80),
    }
    resolved_title = _compact_text(title, max_chars=160) or "Analysis note"
    artifact, cell = _append_artifact_cell(
        workspace,
        task_id=task_id,
        operation=operation,
        artifact_type="analysis_note",
        artifact_schema=ANALYSIS_NOTE_SCHEMA,
        title=resolved_title,
        summary=_compact_text(clean_markdown, max_chars=240),
        payload=payload,
        cell_type="analysis_note",
        dataset_id=(related_dataset_ids or [None])[0],
        depends_on=related_dataset_ids,
    )
    return {
        "operation_id": operation["operation_id"],
        "artifact_id": artifact["artifact_id"],
        "cell_id": cell["cell_id"],
        "payload": payload,
    }


def append_notebook_code_cell_artifact(
    workspace: dict[str, Any],
    *,
    task_id: str,
    user_prompt: str,
    source_code: str,
    outputs: list[Mapping[str, Any]],
    title: str = "",
    summary: str = "",
    cell_kind: str = "analysis_code",
    output_summary: list[str] | None = None,
    context_artifact_ids: list[str] | None = None,
    selected_dataset_id: str | None = None,
) -> dict[str, Any]:
    clean_source = str(source_code or "").strip()
    if not clean_source:
        raise ValueError("source_code must not be empty.")

    resolved_kind = _slugify(cell_kind or "analysis_code", fallback="analysis_code", max_chars=32).replace("-", "_")
    operation = _ensure_operation(
        workspace,
        task_id=task_id,
        user_prompt=user_prompt,
        intent="dataset_metadata" if selected_dataset_id else "dataset_search_compare",
        target_dataset_ids=[selected_dataset_id] if selected_dataset_id else [],
    )
    payload = {
        "schema": ANALYSIS_CODE_CELL_SCHEMA,
        "created_at": _utc_now(),
        "language": "python",
        "kind": resolved_kind,
        "source_code": clean_source,
        "outputs": [dict(item) for item in list(outputs or [])],
        "output_summary": _dedupe_strings(output_summary or [], limit=8, max_chars=80),
        "context_artifact_ids": _dedupe_strings(context_artifact_ids or [], limit=8, max_chars=64),
    }
    resolved_title = _compact_text(title, max_chars=160) or "Notebook analysis cell"
    artifact, cell = _append_artifact_cell(
        workspace,
        task_id=task_id,
        operation=operation,
        artifact_type="analysis_code_cell",
        artifact_schema=ANALYSIS_CODE_CELL_SCHEMA,
        title=resolved_title,
        summary=summary or resolved_title,
        payload=payload,
        cell_type="analysis_code",
        dataset_id=selected_dataset_id or None,
        depends_on=list(payload.get("context_artifact_ids") or []),
    )
    return {
        "operation_id": operation["operation_id"],
        "artifact_id": artifact["artifact_id"],
        "cell_id": cell["cell_id"],
        "payload": payload,
    }


def set_selected_dataset(workspace: dict[str, Any], dataset_id: str) -> bool:
    normalized = str(dataset_id or "").strip()
    if not normalized:
        workspace["selected_dataset_id"] = None
        _bump_workspace_revision(workspace)
        return True
    if normalized not in set((workspace.get("datasets") or {}).keys()):
        return False
    workspace["selected_dataset_id"] = normalized
    _bump_workspace_revision(workspace)
    return True


def analysis_context_payload(workspace: dict[str, Any]) -> dict[str, Any]:
    dataset_index: dict[str, Any] = {}
    for dataset_id, dataset in dict(workspace.get("datasets") or {}).items():
        dataset_index[dataset_id] = {
            "dataset_id": dataset_id,
            "label": dataset.get("label", ""),
            "source": dataset.get("source", ""),
            "accession": dataset.get("accession", ""),
            "tags": list(dataset.get("tags", []) or []),
            "latest_artifact_ids": dict(dataset.get("latest_artifact_ids", {}) or {}),
        }

    recent_cells = []
    for cell in list(workspace.get("cells", []) or [])[-5:]:
        artifact = dict((workspace.get("artifacts") or {}).get(str(cell.get("artifact_id") or "").strip()) or {})
        recent_cells.append(
            {
                "cell_id": cell.get("cell_id", ""),
                "type": cell.get("type", ""),
                "title": cell.get("title", ""),
                "task_id": cell.get("task_id", ""),
                "operation_id": cell.get("operation_id", ""),
                "selected_dataset_id": cell.get("selected_dataset_id"),
                "artifact_summary": artifact.get("summary", ""),
            }
        )

    selected_dataset_id = str(workspace.get("selected_dataset_id") or "").strip()
    selected_dataset = dataset_index.get(selected_dataset_id) if selected_dataset_id else None
    return {
        "schema": ANALYSIS_CONTEXT_SCHEMA,
        "workspace_revision": int(workspace.get("revision", 0) or 0),
        "selected_dataset": selected_dataset,
        "recent_operations": list(workspace.get("operations", []) or [])[-5:],
        "recent_cells": recent_cells,
        "artifact_index_by_dataset": dataset_index,
    }


def task_analysis_snapshot(workspace: dict[str, Any], *, task_id: str) -> dict[str, Any]:
    normalized_task_id = str(task_id or "").strip()
    operation_id = ""
    for operation in reversed(list(workspace.get("operations", []) or [])):
        if str(operation.get("task_id") or "").strip() == normalized_task_id:
            operation_id = str(operation.get("operation_id") or "").strip()
            break
    cell_ids = [
        str(cell.get("cell_id") or "").strip()
        for cell in list(workspace.get("cells", []) or [])
        if str(cell.get("task_id") or "").strip() == normalized_task_id
    ]
    return {
        "analysis_operation_id": operation_id,
        "analysis_cell_ids": cell_ids,
        "analysis_workspace_revision": int(workspace.get("revision", 0) or 0),
        "selected_dataset_id_snapshot": str(workspace.get("selected_dataset_id") or "").strip(),
    }


__all__ = [
    "ANALYSIS_CODE_CELL_SCHEMA",
    "ANALYSIS_CONTEXT_SCHEMA",
    "ANALYSIS_NOTE_SCHEMA",
    "ANALYSIS_WORKSPACE_SCHEMA",
    "DATASET_CATALOG_SCHEMA",
    "DATASET_METADATA_PROFILE_SCHEMA",
    "METADATA_SNAPSHOT_SCHEMA",
    "analysis_context_payload",
    "append_analysis_note_artifact",
    "append_dataset_catalog_artifact",
    "append_metadata_snapshot_artifact",
    "append_dataset_metadata_artifact",
    "append_notebook_code_cell_artifact",
    "build_dataset_catalog_payload",
    "canonical_dataset_id",
    "ensure_analysis_workspace",
    "normalize_dataset_metadata_profile",
    "parse_json_array_text",
    "parse_json_object_text",
    "set_selected_dataset",
    "task_analysis_snapshot",
]
