"""Helpers for storing and rendering dataset-comparison visualizations.

The analysis mode can use this module to persist a compact visualization bundle
that the web UI can render without needing direct access to raw tool payloads.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
from typing import Any
from typing import Mapping

from pydantic import BaseModel, Field, ValidationError


DATASET_VISUALIZATION_SCHEMA = "dataset_visualizations.v1"
MIN_SCORE = 0.0
MAX_SCORE = 5.0
MAX_DIMENSIONS = 6
MAX_DATASETS = 12


class DatasetVisualizationDimension(BaseModel):
    key: str = Field(
        ...,
        description="Machine-friendly dimension key such as disease_match or metadata_quality.",
    )
    label: str = Field(
        ...,
        description="Human-readable label for the dimension.",
    )
    description: str = Field(
        default="",
        description="Short explanation of what the dimension means.",
    )
    weight: float = Field(
        default=1.0,
        ge=0.1,
        le=3.0,
        description="Relative importance of this dimension in the overall score.",
    )
    low_label: str = Field(
        default="Lower",
        description="What low scores mean on this dimension.",
    )
    high_label: str = Field(
        default="Higher",
        description="What high scores mean on this dimension.",
    )


class DatasetVisualizationRow(BaseModel):
    dataset_label: str = Field(
        ...,
        description="Display name of the dataset or archive candidate.",
    )
    source: str = Field(
        default="",
        description="Source/archive name such as OpenNeuro or GEO.",
    )
    accession: str = Field(
        default="",
        description="Optional accession, dataset id, or repository id.",
    )
    notes: str = Field(
        default="",
        description="One brief evidence-grounded note about why this dataset scored the way it did.",
    )
    scores: dict[str, float] = Field(
        default_factory=dict,
        description="Dimension-to-score mapping using coarse 0-5 values, where higher is better.",
    )
    metrics: dict[str, float | int | str | bool] = Field(
        default_factory=dict,
        description="Optional scalar metadata such as subject_count, sample_count, or size_gb.",
    )
    tags: list[str] = Field(
        default_factory=list,
        description="Short labels such as MRI, schizophrenia, BIDS, or public download.",
    )
    evidence_ids: list[str] = Field(
        default_factory=list,
        description="Optional supporting identifiers such as PMIDs or dataset ids.",
    )


def _summarize_validation_error(exc: ValidationError) -> str:
    parts: list[str] = []
    for error in exc.errors()[:3]:
        loc = ".".join(str(token) for token in error.get("loc", ()) if token not in {"__root__"})
        msg = str(error.get("msg", "Invalid value")).strip() or "Invalid value"
        parts.append(f"{loc}: {msg}" if loc else msg)
    return "; ".join(parts) or "Invalid value"


def _coerce_dimension(value: Any, *, index: int) -> DatasetVisualizationDimension:
    if isinstance(value, DatasetVisualizationDimension):
        return value
    if not isinstance(value, Mapping):
        raise ValueError(f"Invalid dimensions[{index}]: expected an object.")
    try:
        return DatasetVisualizationDimension.model_validate(dict(value))
    except ValidationError as exc:
        raise ValueError(
            f"Invalid dimensions[{index}]: {_summarize_validation_error(exc)}."
        ) from exc


def _coerce_row(value: Any, *, index: int) -> DatasetVisualizationRow:
    if isinstance(value, DatasetVisualizationRow):
        return value
    if not isinstance(value, Mapping):
        raise ValueError(f"Invalid datasets[{index}]: expected an object.")
    try:
        return DatasetVisualizationRow.model_validate(dict(value))
    except ValidationError as exc:
        raise ValueError(
            f"Invalid datasets[{index}]: {_summarize_validation_error(exc)}."
        ) from exc


def coerce_dataset_visualization_dimensions(values: list[Any] | None) -> list[DatasetVisualizationDimension]:
    return [
        _coerce_dimension(value, index=idx)
        for idx, value in enumerate(list(values or []))
    ]


def coerce_dataset_visualization_rows(values: list[Any] | None) -> list[DatasetVisualizationRow]:
    return [
        _coerce_row(value, index=idx)
        for idx, value in enumerate(list(values or []))
    ]


def _strip_json_code_fence(text: str) -> str:
    stripped = str(text or "").strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if not lines:
        return stripped
    if lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def parse_visualization_json_array(value: Any, *, field_name: str) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)

    text = _strip_json_code_fence(str(value or ""))
    if not text:
        return []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"{field_name} must be valid JSON array text: {exc.msg}."
        ) from exc

    if not isinstance(parsed, list):
        raise ValueError(f"{field_name} must decode to a JSON array.")
    return parsed


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _slugify(value: Any) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    return text[:48]


def _compact_text(value: Any, *, max_chars: int = 240) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= max_chars:
        return text
    return f"{text[: max_chars - 3].rstrip()}..."


def _dedupe_str_list(values: list[Any], *, limit: int = 12) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for raw in values:
        text = _compact_text(raw, max_chars=80)
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


def _coerce_score(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        score = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(score) or math.isinf(score):
        return None
    return round(min(MAX_SCORE, max(MIN_SCORE, score)), 2)


def _coerce_metric(value: Any) -> float | int | str | bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(value, 3)
    text = _compact_text(value, max_chars=60)
    return text or None


def _variance(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    return sum((value - mean) ** 2 for value in values) / len(values)


def _metric_candidates(rows: list[dict[str, Any]]) -> list[str]:
    candidates: dict[str, list[float]] = {}
    for row in rows:
        metrics = row.get("metrics", {})
        if not isinstance(metrics, dict):
            continue
        for key, value in metrics.items():
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                candidates.setdefault(str(key), []).append(float(value))
    ranked = sorted(
        candidates.items(),
        key=lambda item: (_variance(item[1]), len(item[1])),
        reverse=True,
    )
    return [key for key, values in ranked if len(values) >= 2 and _variance(values) > 0.0][:4]


def build_dataset_visualization_bundle(
    *,
    objective: str = "",
    summary: str = "",
    dimensions: list[Any],
    datasets: list[Any],
    notes: list[str] | None = None,
) -> dict[str, Any]:
    """Validate dataset-comparison rows and convert them into a UI bundle."""
    coerced_dimensions = coerce_dataset_visualization_dimensions(dimensions)
    coerced_rows = coerce_dataset_visualization_rows(datasets)
    normalized_dimensions: list[dict[str, Any]] = []
    seen_dimension_keys: set[str] = set()
    for idx, dimension in enumerate(coerced_dimensions[:MAX_DIMENSIONS], start=1):
        key = _slugify(dimension.key) or f"dimension_{idx}"
        if key in seen_dimension_keys:
            continue
        seen_dimension_keys.add(key)
        normalized_dimensions.append(
            {
                "key": key,
                "label": _compact_text(dimension.label, max_chars=36) or f"Dimension {idx}",
                "description": _compact_text(dimension.description, max_chars=120),
                "weight": float(dimension.weight or 1.0),
                "low_label": _compact_text(dimension.low_label, max_chars=20) or "Lower",
                "high_label": _compact_text(dimension.high_label, max_chars=20) or "Higher",
            }
        )

    if len(normalized_dimensions) < 2:
        raise ValueError("Need at least two comparison dimensions to build dataset visualizations.")

    warnings: list[str] = []
    normalized_rows: list[dict[str, Any]] = []
    for row in coerced_rows[:MAX_DATASETS]:
        dataset_label = _compact_text(row.dataset_label, max_chars=72)
        if not dataset_label:
            warnings.append("Skipped one dataset row because the dataset_label was empty.")
            continue

        row_scores = row.scores if isinstance(row.scores, dict) else {}
        normalized_scores: dict[str, float] = {}
        missing_dimensions: list[str] = []
        for dimension in normalized_dimensions:
            candidates = (
                row_scores.get(dimension["key"]),
                row_scores.get(dimension["label"]),
                row_scores.get(str(dimension["key"]).replace("_", " ")),
            )
            score = next((coerced for coerced in (_coerce_score(value) for value in candidates) if coerced is not None), None)
            if score is None:
                missing_dimensions.append(dimension["label"])
                continue
            normalized_scores[dimension["key"]] = score

        if missing_dimensions:
            warnings.append(
                f"Skipped {dataset_label} because scores were missing for: {', '.join(missing_dimensions[:4])}."
            )
            continue

        metrics: dict[str, float | int | str | bool] = {}
        for key, value in (row.metrics or {}).items():
            normalized = _coerce_metric(value)
            if normalized is None:
                continue
            metrics[_slugify(key) or str(key)] = normalized

        normalized_rows.append(
            {
                "dataset_label": dataset_label,
                "short_label": _compact_text(dataset_label, max_chars=28),
                "source": _compact_text(row.source, max_chars=32),
                "accession": _compact_text(row.accession, max_chars=32),
                "notes": _compact_text(row.notes, max_chars=220),
                "scores": normalized_scores,
                "metrics": metrics,
                "tags": _dedupe_str_list(list(row.tags or []), limit=8),
                "evidence_ids": _dedupe_str_list(list(row.evidence_ids or []), limit=8),
            }
        )

    if len(normalized_rows) < 2:
        raise ValueError("Need at least two well-formed dataset rows to build visualizations.")

    total_weight = sum(float(dimension["weight"]) for dimension in normalized_dimensions) or 1.0
    for row in normalized_rows:
        weighted_sum = sum(
            float(row["scores"][dimension["key"]]) * float(dimension["weight"])
            for dimension in normalized_dimensions
        )
        row["overall_score"] = round(weighted_sum / total_weight, 2)

    normalized_rows.sort(key=lambda row: row["overall_score"], reverse=True)

    dimension_variances = {
        dimension["key"]: _variance([float(row["scores"][dimension["key"]]) for row in normalized_rows])
        for dimension in normalized_dimensions
    }
    ranked_dimensions = sorted(
        normalized_dimensions,
        key=lambda dimension: (
            dimension_variances.get(dimension["key"], 0.0),
            float(dimension["weight"]),
        ),
        reverse=True,
    )
    scatter_x = ranked_dimensions[0]["key"]
    scatter_y = ranked_dimensions[1]["key"]
    metric_keys = _metric_candidates(normalized_rows)
    if metric_keys:
        scatter_size_key = metric_keys[0]
        scatter_size_kind = "metric"
    elif len(ranked_dimensions) >= 3:
        scatter_size_key = ranked_dimensions[2]["key"]
        scatter_size_kind = "dimension"
    else:
        scatter_size_key = "overall_score"
        scatter_size_kind = "overall"

    unique_sources = {
        str(row.get("source", "")).strip()
        for row in normalized_rows
        if str(row.get("source", "")).strip()
    }
    top_dataset = normalized_rows[0]

    return {
        "schema": DATASET_VISUALIZATION_SCHEMA,
        "generated_at": _utc_now(),
        "objective": _compact_text(objective, max_chars=240),
        "summary": _compact_text(summary, max_chars=320),
        "summary_cards": {
            "dataset_count": len(normalized_rows),
            "dimension_count": len(normalized_dimensions),
            "source_count": len(unique_sources),
            "top_dataset": top_dataset["dataset_label"],
            "top_score": top_dataset["overall_score"],
        },
        "dimensions": normalized_dimensions,
        "rows": normalized_rows,
        "charts": {
            "bar": {
                "title": "Overall suitability score",
                "description": "Weighted mean of the scored comparison dimensions.",
                "score_key": "overall_score",
                "max_score": MAX_SCORE,
            },
            "heatmap": {
                "title": "Dataset fit by dimension",
                "description": "Coarse evidence-grounded scores by comparison dimension.",
                "max_score": MAX_SCORE,
            },
            "scatter": {
                "title": "Dataset comparison landscape",
                "description": "Higher-right datasets score better on both plotted dimensions.",
                "x_dimension": scatter_x,
                "y_dimension": scatter_y,
                "size_key": scatter_size_key,
                "size_kind": scatter_size_kind,
                "max_score": MAX_SCORE,
            },
        },
        "notes": _dedupe_str_list(list(notes or []), limit=8),
        "warnings": warnings[:8],
    }


__all__ = [
    "DATASET_VISUALIZATION_SCHEMA",
    "DatasetVisualizationDimension",
    "DatasetVisualizationRow",
    "build_dataset_visualization_bundle",
    "coerce_dataset_visualization_dimensions",
    "coerce_dataset_visualization_rows",
    "parse_visualization_json_array",
]
