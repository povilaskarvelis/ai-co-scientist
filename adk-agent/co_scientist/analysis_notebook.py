"""Build a real nbformat notebook from the analysis workspace."""

from __future__ import annotations

from datetime import datetime, timezone
import html
import json
import uuid
from typing import Any
from typing import Mapping

try:
    from nbformat import from_dict
    from nbformat import writes
    from nbformat import v4 as nbf
except ImportError:  # pragma: no cover - exercised in user runtime when nbformat is absent
    from_dict = None
    writes = None
    nbf = None


ANALYSIS_NOTEBOOK_SCHEMA = "analysis_notebook.v1"

# Rows included in dataset_comparison code cells and SVG previews (executor fallback can exceed 12).
NOTEBOOK_COMPARISON_MAX_DATASETS = 60


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: Any, *, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _slug(value: Any) -> str:
    text = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value or "").strip())
    text = "_".join(part for part in text.split("_") if part)
    return text[:64] or "value"


def _escape(value: Any) -> str:
    return html.escape(str(value or ""))


def _pretty_json(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=True, sort_keys=True)


def _cell_id() -> str:
    return uuid.uuid4().hex[:8]


def _new_output(*, output_type: str, data: Mapping[str, Any] | None = None, text: str = "", name: str = "") -> dict[str, Any]:
    if nbf is not None:
        kwargs: dict[str, Any] = {"output_type": output_type}
        if data is not None:
            kwargs["data"] = dict(data)
        if text:
            kwargs["text"] = text
        if name:
            kwargs["name"] = name
        return dict(nbf.new_output(**kwargs))
    output: dict[str, Any] = {"output_type": output_type}
    if data is not None:
        output["data"] = dict(data)
        output["metadata"] = {}
    if text:
        output["text"] = text
    if name:
        output["name"] = name
    return output


def _new_markdown_cell(source: str, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if nbf is not None:
        return dict(nbf.new_markdown_cell(source, metadata=dict(metadata or {})))
    return {
        "cell_type": "markdown",
        "id": _cell_id(),
        "metadata": dict(metadata or {}),
        "source": source,
    }


def _new_code_cell(
    source: str,
    *,
    execution_count: int | None = None,
    outputs: list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if nbf is not None:
        return dict(
            nbf.new_code_cell(
                source=source,
                execution_count=execution_count,
                outputs=list(outputs or []),
                metadata=dict(metadata or {}),
            )
        )
    return {
        "cell_type": "code",
        "execution_count": execution_count,
        "id": _cell_id(),
        "metadata": dict(metadata or {}),
        "outputs": [dict(output) for output in list(outputs or [])],
        "source": source,
    }


def _new_notebook(*, cells: list[Mapping[str, Any]], metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if nbf is not None:
        return dict(nbf.new_notebook(cells=list(cells), metadata=dict(metadata or {})))
    return {
        "cells": [dict(cell) for cell in cells],
        "metadata": dict(metadata or {}),
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _metric_value(metrics: Mapping[str, Any] | None, key: str) -> Any:
    if not isinstance(metrics, Mapping):
        return None
    if key in metrics:
        return metrics.get(key)
    alt_key = key.replace("_", "-")
    if alt_key in metrics:
        return metrics.get(alt_key)
    return None


def _dimension_key(dimension: Any) -> str:
    if isinstance(dimension, Mapping):
        return _text(dimension.get("key") or dimension.get("label"))
    return _text(dimension)


def _dimension_label(dimension: Any) -> str:
    if isinstance(dimension, Mapping):
        return _text(dimension.get("label") or dimension.get("key"))
    return _text(dimension).replace("_", " ")


def _metadata_base(*, cell: Mapping[str, Any], artifact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "co_scientist": {
            "task_id": _text(cell.get("task_id")),
            "cell_id": _text(cell.get("cell_id")),
            "artifact_id": _text(artifact.get("artifact_id")),
            "artifact_type": _text(artifact.get("type")),
            "artifact_schema": _text(artifact.get("schema")),
            "created_at": _text(cell.get("created_at") or artifact.get("created_at")),
            "title": _text(cell.get("title") or artifact.get("title")),
        }
    }


def _output_html_block(title: str, body: str) -> str:
    return (
        "<div class='co-output-block'>"
        f"<div class='co-output-title'>{_escape(title)}</div>"
        f"{body}"
        "</div>"
    )


def _table_html(headers: list[str], rows: list[list[str]]) -> str:
    thead = "".join(f"<th>{_escape(header)}</th>" for header in headers)
    tbody = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return (
        "<table class='co-table'>"
        f"<thead><tr>{thead}</tr></thead>"
        f"<tbody>{tbody}</tbody>"
        "</table>"
    )


def _list_text(value: Any) -> list[str]:
    if isinstance(value, (list, tuple, set)):
        items = [str(item).strip() for item in value if str(item).strip()]
        return items
    text = _text(value)
    if not text:
        return []
    return [item.strip() for item in text.split(",") if item.strip()]


def _catalog_bool(value: Any) -> str:
    if value is True:
        return "Yes"
    if value is False:
        return "No"
    text = _text(value)
    return text or "n/a"


def _catalog_doi(dataset: Mapping[str, Any]) -> str:
    direct = _text(dataset.get("doi"))
    if direct:
        return direct
    for item in list(dataset.get("evidence_ids", []) or []):
        text = _text(item)
        if text.upper().startswith("DOI:"):
            return text.split(":", 1)[1].strip()
    return ""


def _catalog_record(dataset: Mapping[str, Any]) -> dict[str, Any]:
    metrics = dataset.get("metrics", {}) if isinstance(dataset.get("metrics"), Mapping) else {}
    participant_count = (
        dataset.get("participant_count")
        or _metric_value(metrics, "participant_count")
        or _metric_value(metrics, "subject_count")
    )
    session_count = dataset.get("session_count") or _metric_value(metrics, "session_count")
    modalities = _list_text(dataset.get("modalities"))
    if not modalities:
        modalities = [
            str(tag).replace("modality:", "").strip()
            for tag in list(dataset.get("tags", []) or [])
            if str(tag).strip().lower().startswith("modality:")
            or str(tag).strip().upper() in {"MRI", "FMRI", "RSFMRI", "EEG", "MEG", "PET", "IEEG"}
        ]
    diagnoses = _list_text(dataset.get("diagnoses"))
    tasks = _list_text(dataset.get("tasks"))
    doi = _catalog_doi(dataset)
    license_text = _text(dataset.get("license"))
    record = {
        "dataset_label": _text(dataset.get("dataset_label")),
        "accession": _text(dataset.get("accession")),
        "source": _text(dataset.get("source")),
        "participant_count": participant_count,
        "session_count": session_count,
        "modalities": modalities,
        "diagnoses": diagnoses,
        "tasks": tasks,
        "public_download": dataset.get("public_download"),
        "license": license_text,
        "doi": doi,
        "bids": dataset.get("bids"),
        "notes": _text(dataset.get("notes") or dataset.get("description")),
    }
    return record


def _dataset_comparison_source(payload: Mapping[str, Any], artifact: Mapping[str, Any]) -> str:
    del artifact
    records = [_catalog_record(dataset) for dataset in list(payload.get("datasets", []) or [])[:NOTEBOOK_COMPARISON_MAX_DATASETS]]
    return (
        "import pandas as pd\n"
        "import matplotlib.pyplot as plt\n\n"
        f"records = {_pretty_json(records)}\n\n"
        "df = pd.DataFrame(records)\n\n"
        "def present(value):\n"
        "    if value is None:\n"
        "        return False\n"
        "    if isinstance(value, float) and pd.isna(value):\n"
        "        return False\n"
        "    if isinstance(value, (list, tuple, set, dict)):\n"
        "        return len(value) > 0\n"
        "    return str(value).strip() != ''\n\n"
        "def plot_binary_matrix(matrix_df, title, cmap='Blues'):\n"
        "    if matrix_df.empty:\n"
        "        return\n"
        "    fig_w = max(7, 0.65 * len(matrix_df.columns) + 4)\n"
        "    fig_h = max(3, 0.5 * len(matrix_df.index) + 2)\n"
        "    fig, ax = plt.subplots(figsize=(fig_w, fig_h))\n"
        "    ax.imshow(matrix_df.values, cmap=cmap, aspect='auto', vmin=0, vmax=1)\n"
        "    ax.set_xticks(range(len(matrix_df.columns)))\n"
        "    ax.set_xticklabels(matrix_df.columns, rotation=35, ha='right')\n"
        "    ax.set_yticks(range(len(matrix_df.index)))\n"
        "    ax.set_yticklabels(matrix_df.index)\n"
        "    ax.set_title(title)\n"
        "    for i in range(matrix_df.shape[0]):\n"
        "        for j in range(matrix_df.shape[1]):\n"
        "            ax.text(j, i, int(matrix_df.iat[i, j]), ha='center', va='center', color='white' if matrix_df.iat[i, j] else '#475569', fontsize=9)\n"
        "    plt.tight_layout()\n"
        "    plt.show()\n\n"
        "def multi_hot_matrix(series):\n"
        "    labels = []\n"
        "    for items in series:\n"
        "        if not isinstance(items, list):\n"
        "            continue\n"
        "        for item in items:\n"
        "            if item and item not in labels:\n"
        "                labels.append(item)\n"
        "    if not labels:\n"
        "        return pd.DataFrame(index=df['accession'].replace('', pd.NA).fillna(df['dataset_label']))\n"
        "    matrix = pd.DataFrame(0, index=df['accession'].replace('', pd.NA).fillna(df['dataset_label']), columns=labels)\n"
        "    for idx, items in enumerate(series):\n"
        "        if not isinstance(items, list):\n"
        "            continue\n"
        "        row_label = matrix.index[idx]\n"
        "        for item in items:\n"
        "            if item in matrix.columns:\n"
        "                matrix.loc[row_label, item] = 1\n"
        "    return matrix\n\n"
        "field_matrix = pd.DataFrame({\n"
        "    'Participants': df['participant_count'].apply(lambda value: int(present(value))) if 'participant_count' in df.columns else 0,\n"
        "    'Sessions': df['session_count'].apply(lambda value: int(present(value))) if 'session_count' in df.columns else 0,\n"
        "    'Modalities': df['modalities'].apply(lambda value: int(present(value))) if 'modalities' in df.columns else 0,\n"
        "    'Diagnoses': df['diagnoses'].apply(lambda value: int(present(value))) if 'diagnoses' in df.columns else 0,\n"
        "    'Tasks': df['tasks'].apply(lambda value: int(present(value))) if 'tasks' in df.columns else 0,\n"
        "    'Public': df['public_download'].apply(lambda value: int(present(value))) if 'public_download' in df.columns else 0,\n"
        "    'License': df['license'].apply(lambda value: int(present(value))) if 'license' in df.columns else 0,\n"
        "    'DOI': df['doi'].apply(lambda value: int(present(value))) if 'doi' in df.columns else 0,\n"
        "    'BIDS': df['bids'].apply(lambda value: int(present(value))) if 'bids' in df.columns else 0,\n"
        "}, index=df['accession'].replace('', pd.NA).fillna(df['dataset_label']))\n"
        "plot_binary_matrix(field_matrix, 'Metadata field presence matrix', cmap='Blues')\n\n"
        "if 'participant_count' in df.columns and df['participant_count'].notna().any():\n"
        "    participant_df = df.dropna(subset=['participant_count']).sort_values('participant_count')\n"
        "    plt.figure(figsize=(10, max(3, 0.55 * len(participant_df))))\n"
        "    plt.barh(participant_df['dataset_label'], participant_df['participant_count'], color='#4C78A8')\n"
        "    plt.title('Participants per dataset')\n"
        "    plt.xlabel('Participant count')\n"
        "    plt.tight_layout()\n"
        "    plt.show()\n\n"
        "if 'modalities' in df.columns:\n"
        "    modality_matrix = multi_hot_matrix(df['modalities'])\n"
        "    if not modality_matrix.empty and len(modality_matrix.columns):\n"
        "        plot_binary_matrix(modality_matrix, 'Modality-by-dataset matrix', cmap='GnBu')\n\n"
        "if 'tasks' in df.columns:\n"
        "    task_matrix = multi_hot_matrix(df['tasks'])\n"
        "    if not task_matrix.empty and len(task_matrix.columns):\n"
        "        plot_binary_matrix(task_matrix, 'Task-by-dataset matrix', cmap='Purples')\n\n"
        "if 'diagnoses' in df.columns:\n"
        "    diagnosis_matrix = multi_hot_matrix(df['diagnoses'])\n"
        "    if not diagnosis_matrix.empty and len(diagnosis_matrix.columns):\n"
        "        plot_binary_matrix(diagnosis_matrix, 'Diagnosis-by-dataset matrix', cmap='Oranges')\n\n"
        "status_matrix = pd.DataFrame({\n"
        "    'Public': df['public_download'].apply(lambda value: int(present(value))) if 'public_download' in df.columns else 0,\n"
        "    'BIDS': df['bids'].apply(lambda value: int(present(value))) if 'bids' in df.columns else 0,\n"
        "    'DOI': df['doi'].apply(lambda value: int(present(value))) if 'doi' in df.columns else 0,\n"
        "    'License': df['license'].apply(lambda value: int(present(value))) if 'license' in df.columns else 0,\n"
        "}, index=df['accession'].replace('', pd.NA).fillna(df['dataset_label']))\n"
        "plot_binary_matrix(status_matrix, 'Access and standards matrix', cmap='RdPu')\n"
    )


def _numeric_chart_svg(rows: list[dict[str, Any]], *, key: str, title: str, color: str) -> str:
    clean_rows: list[dict[str, Any]] = []
    for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        value = row.get(key)
        if value in (None, "", [], {}):
            continue
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        clean_rows.append(
            {
                "label": _escape(row.get("accession") or row.get("dataset_label") or ""),
                "value": numeric,
            }
        )
    if not clean_rows:
        return ""
    width = 760
    row_height = 30
    left = 210
    top = 28
    chart_width = width - left - 36
    max_value = max(item["value"] for item in clean_rows) or 1.0
    height = top + len(clean_rows) * row_height + 22
    svg_rows = [
        f"<text x='{left}' y='18' font-size='14' font-weight='700' fill='#0f172a'>{_escape(title)}</text>"
    ]
    for idx, row in enumerate(clean_rows):
        y = top + idx * row_height
        bar_w = max(4.0, chart_width * row["value"] / max_value)
        svg_rows.append(
            f"<text x='{left - 10}' y='{y + 15}' font-size='12' text-anchor='end' fill='#334155'>{row['label']}</text>"
        )
        svg_rows.append(
            f"<rect x='{left}' y='{y + 3}' width='{chart_width:.2f}' height='14' rx='7' fill='#e2e8f0'></rect>"
        )
        svg_rows.append(
            f"<rect x='{left}' y='{y + 3}' width='{bar_w:.2f}' height='14' rx='7' fill='{color}'></rect>"
        )
        svg_rows.append(
            f"<text x='{left + chart_width + 8}' y='{y + 15}' font-size='12' fill='#0f172a'>{row['value']:.2f}</text>"
        )
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='white'></rect>"
        + "".join(svg_rows)
        + "</svg>"
    )


def _binary_matrix_svg(
    *,
    row_labels: list[str],
    column_labels: list[str],
    values: list[list[int]],
    title: str,
    on_color: str,
    off_color: str = "#eef2f7",
) -> str:
    if not row_labels or not column_labels or not values:
        return ""
    cell = 28
    left = 210
    top = 74
    width = max(760, left + len(column_labels) * cell + 24)
    height = top + len(row_labels) * cell + 20
    svg_rows = [
        f"<text x='{left}' y='22' font-size='14' font-weight='700' fill='#0f172a'>{_escape(title)}</text>"
    ]
    for idx, label in enumerate(column_labels):
        x = left + idx * cell + cell / 2
        svg_rows.append(
            f"<text x='{x:.1f}' y='58' font-size='11' text-anchor='middle' fill='#334155' transform='rotate(-35 {x:.1f} 58)'>{_escape(label)}</text>"
        )
    for r_idx, label in enumerate(row_labels):
        y = top + r_idx * cell + cell / 2 + 4
        svg_rows.append(
            f"<text x='{left - 10}' y='{y:.1f}' font-size='12' text-anchor='end' fill='#334155'>{_escape(label)}</text>"
        )
        for c_idx, value in enumerate(values[r_idx]):
            x = left + c_idx * cell
            y_rect = top + r_idx * cell
            fill = on_color if int(value or 0) else off_color
            text_fill = "#ffffff" if int(value or 0) else "#64748b"
            label_text = "1" if int(value or 0) else "0"
            svg_rows.append(
                f"<rect x='{x}' y='{y_rect}' width='{cell - 2}' height='{cell - 2}' rx='6' fill='{fill}' stroke='#d7dee8' stroke-width='1'></rect>"
            )
            svg_rows.append(
                f"<text x='{x + (cell - 2) / 2:.1f}' y='{y_rect + cell / 2 + 3:.1f}' font-size='11' text-anchor='middle' fill='{text_fill}'>{label_text}</text>"
            )
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='white'></rect>"
        + "".join(svg_rows)
        + "</svg>"
    )


def _category_chart_svg(rows: list[dict[str, Any]], *, key: str, title: str, color: str) -> str:
    counts: dict[str, int] = {}
    for row in rows:
        for item in _list_text(row.get(key)):
            counts[item] = counts.get(item, 0) + 1
    if not counts:
        return ""
    sorted_counts = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    width = 760
    row_height = 30
    left = 210
    top = 28
    chart_width = width - left - 36
    max_value = max(value for _, value in sorted_counts) or 1
    height = top + len(sorted_counts) * row_height + 22
    svg_rows = [
        f"<text x='{left}' y='18' font-size='14' font-weight='700' fill='#0f172a'>{_escape(title)}</text>"
    ]
    for idx, (label, value) in enumerate(sorted_counts):
        y = top + idx * row_height
        bar_w = max(4.0, chart_width * value / max_value)
        svg_rows.append(
            f"<text x='{left - 10}' y='{y + 15}' font-size='12' text-anchor='end' fill='#334155'>{_escape(label)}</text>"
        )
        svg_rows.append(
            f"<rect x='{left}' y='{y + 3}' width='{chart_width:.2f}' height='14' rx='7' fill='#e2e8f0'></rect>"
        )
        svg_rows.append(
            f"<rect x='{left}' y='{y + 3}' width='{bar_w:.2f}' height='14' rx='7' fill='{color}'></rect>"
        )
        svg_rows.append(
            f"<text x='{left + chart_width + 8}' y='{y + 15}' font-size='12' fill='#0f172a'>{value}</text>"
        )
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='white'></rect>"
        + "".join(svg_rows)
        + "</svg>"
    )


def _field_presence_svg(rows: list[dict[str, Any]], *, fields: list[tuple[str, str]], title: str) -> str:
    row_labels = [_text(row.get("accession") or row.get("dataset_label") or "Dataset") for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]]
    column_labels = [label for _, label in fields]
    values: list[list[int]] = []
    for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        row_values: list[int] = []
        for key, _label in fields:
            value = row.get(key)
            row_values.append(0 if value in (None, "", [], {}) else 1)
        values.append(row_values)
    return _binary_matrix_svg(
        row_labels=row_labels,
        column_labels=column_labels,
        values=values,
        title=title,
        on_color="#2563eb",
    )


def _multi_value_matrix_svg(rows: list[dict[str, Any]], *, key: str, title: str, color: str) -> str:
    labels: list[str] = []
    for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        for item in _list_text(row.get(key)):
            if item not in labels:
                labels.append(item)
    labels = labels[:10]
    if not labels:
        return ""
    row_labels = [_text(row.get("accession") or row.get("dataset_label") or "Dataset") for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]]
    values: list[list[int]] = []
    for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        items = set(_list_text(row.get(key)))
        values.append([1 if label in items else 0 for label in labels])
    return _binary_matrix_svg(
        row_labels=row_labels,
        column_labels=labels,
        values=values,
        title=title,
        on_color=color,
    )


def _status_matrix_svg(rows: list[dict[str, Any]]) -> str:
    fields = [
        ("public_download", "Public"),
        ("bids", "BIDS"),
        ("doi", "DOI"),
        ("license", "License"),
        ("participant_count", "Participants"),
        ("session_count", "Sessions"),
    ]
    return _field_presence_svg(rows, fields=fields, title="Access and metadata presence matrix")


def _scatter_svg(rows: list[dict[str, Any]], *, x_key: str, y_key: str, title: str) -> str:
    points: list[dict[str, Any]] = []
    for row in rows[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        x_val = row.get(x_key)
        y_val = row.get(y_key)
        try:
            x_num = float(x_val)
            y_num = float(y_val)
        except (TypeError, ValueError):
            continue
        points.append(
            {
                "label": _escape(row.get("accession") or row.get("dataset_label") or ""),
                "x": x_num,
                "y": y_num,
            }
        )
    if len(points) < 2:
        return ""
    width = 760
    height = 420
    left = 70
    right = 22
    top = 30
    bottom = 48
    plot_w = width - left - right
    plot_h = height - top - bottom
    x_max = max(point["x"] for point in points) or 1.0
    y_max = max(point["y"] for point in points) or 1.0
    svg_rows = [
        f"<text x='{left}' y='18' font-size='14' font-weight='700' fill='#0f172a'>{_escape(title)}</text>",
        f"<line x1='{left}' y1='{height - bottom}' x2='{width - right}' y2='{height - bottom}' stroke='#94a3b8' stroke-width='1' />",
        f"<line x1='{left}' y1='{top}' x2='{left}' y2='{height - bottom}' stroke='#94a3b8' stroke-width='1' />",
    ]
    for point in points:
        x = left + (point["x"] / x_max) * plot_w
        y = height - bottom - (point["y"] / y_max) * plot_h
        svg_rows.append(f"<circle cx='{x:.2f}' cy='{y:.2f}' r='5' fill='#54A24B' />")
        svg_rows.append(f"<text x='{x + 7:.2f}' y='{y - 4:.2f}' font-size='11' fill='#0f172a'>{point['label']}</text>")
    return (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{width}' height='{height}' viewBox='0 0 {width} {height}'>"
        "<rect width='100%' height='100%' fill='white'></rect>"
        + "".join(svg_rows)
        + "</svg>"
    )


def _dataset_comparison_preview_text(records: list[dict[str, Any]]) -> str:
    headers = [
        "dataset_label",
        "source",
        "accession",
        "participant_count",
        "session_count",
        "modalities",
        "diagnoses",
        "public_download",
        "license",
        "doi",
        "bids",
    ]
    lines = ["\t".join(headers)]
    for record in records[:NOTEBOOK_COMPARISON_MAX_DATASETS]:
        lines.append(
            "\t".join(
                [
                    _text(record.get("dataset_label")),
                    _text(record.get("source")),
                    _text(record.get("accession")),
                    _text(record.get("participant_count")),
                    _text(record.get("session_count")),
                    ", ".join(_list_text(record.get("modalities"))),
                    ", ".join(_list_text(record.get("diagnoses"))),
                    _catalog_bool(record.get("public_download")),
                    _text(record.get("license")),
                    _text(record.get("doi")),
                    _catalog_bool(record.get("bids")),
                ]
            )
        )
    return "\n".join(lines)


def _dataset_comparison_outputs(payload: Mapping[str, Any]) -> list[Any]:
    records = [_catalog_record(dataset) for dataset in list(payload.get("datasets", []) or [])[:NOTEBOOK_COMPARISON_MAX_DATASETS]]
    if not records:
        return []
    outputs = [
        _new_output(
            output_type="display_data",
            data={
                "text/plain": _dataset_comparison_preview_text(records),
            },
        )
    ]
    presence_svg = _field_presence_svg(
        records,
        fields=[
            ("participant_count", "Participants"),
            ("session_count", "Sessions"),
            ("modalities", "Modalities"),
            ("diagnoses", "Diagnoses"),
            ("tasks", "Tasks"),
            ("public_download", "Public"),
            ("license", "License"),
            ("doi", "DOI"),
            ("bids", "BIDS"),
        ],
        title="Metadata field presence matrix",
    )
    if presence_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": presence_svg,
                    "text/plain": "Metadata field presence matrix",
                },
            )
        )
    participant_svg = _numeric_chart_svg(
        records,
        key="participant_count",
        title="Participants per dataset",
        color="#4C78A8",
    )
    if participant_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": participant_svg,
                    "text/plain": "Participants per dataset",
                },
            )
        )
    modality_matrix_svg = _multi_value_matrix_svg(
        records,
        key="modalities",
        title="Modality-by-dataset matrix",
        color="#0ea5b7",
    )
    if modality_matrix_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": modality_matrix_svg,
                    "text/plain": "Modality-by-dataset matrix",
                },
            )
        )
    modality_svg = _category_chart_svg(
        records,
        key="modalities",
        title="Datasets per modality",
        color="#72B7B2",
    )
    if modality_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": modality_svg,
                    "text/plain": "Datasets per modality",
                },
            )
        )
    task_matrix_svg = _multi_value_matrix_svg(
        records,
        key="tasks",
        title="Task-by-dataset matrix",
        color="#8b5cf6",
    )
    if task_matrix_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": task_matrix_svg,
                    "text/plain": "Task-by-dataset matrix",
                },
            )
        )
    task_svg = _category_chart_svg(
        records,
        key="tasks",
        title="Datasets per task label",
        color="#B279A2",
    )
    if task_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": task_svg,
                    "text/plain": "Datasets per task label",
                },
            )
        )
    diagnosis_matrix_svg = _multi_value_matrix_svg(
        records,
        key="diagnoses",
        title="Diagnosis-by-dataset matrix",
        color="#f59e0b",
    )
    if diagnosis_matrix_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": diagnosis_matrix_svg,
                    "text/plain": "Diagnosis-by-dataset matrix",
                },
            )
        )
    diagnosis_svg = _category_chart_svg(
        records,
        key="diagnoses",
        title="Datasets per diagnosis label",
        color="#F58518",
    )
    if diagnosis_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": diagnosis_svg,
                    "text/plain": "Datasets per diagnosis label",
                },
            )
        )
    status_matrix_svg = _status_matrix_svg(records)
    if status_matrix_svg:
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "image/svg+xml": status_matrix_svg,
                    "text/plain": "Access and standards matrix",
                },
            )
        )
    notes = [str(note).strip() for note in list(payload.get("notes", []) or []) if str(note).strip()]
    warnings = [str(note).strip() for note in list(payload.get("warnings", []) or []) if str(note).strip()]
    if notes or warnings:
        body = ""
        if notes:
            body += "<ul>" + "".join(f"<li>{_escape(note)}</li>" for note in notes) + "</ul>"
        if warnings:
            body += "<div><strong>Warnings</strong><ul>" + "".join(f"<li>{_escape(note)}</li>" for note in warnings) + "</ul></div>"
        outputs.append(
            _new_output(
                output_type="display_data",
                data={
                    "text/plain": "\n".join(notes + warnings),
                    "text/html": _output_html_block("Notes", body),
                },
            )
        )
    return outputs


def _metadata_source(payload: Mapping[str, Any], artifact: Mapping[str, Any]) -> str:
    metadata_obj = {
        "dataset_id": payload.get("dataset_id"),
        "dataset_label": payload.get("dataset_label"),
        "summary": payload.get("summary") or artifact.get("summary"),
        "identity": payload.get("identity", {}),
        "access_download": payload.get("access_download", {}),
        "subjects_sessions": payload.get("subjects_sessions", {}),
        "modalities_tasks": payload.get("modalities_tasks", {}),
        "standards_file_formats": payload.get("standards_file_formats", {}),
        "links_publications": payload.get("links_publications", {}),
    }
    return (
        "# Auto-generated analysis notebook cell\n"
        "# Derived from stored dataset metadata artifacts\n\n"
        f"dataset_metadata = {_pretty_json(metadata_obj)}\n\n"
        "dataset_metadata"
    )


def _section_table_html(title: str, mapping: Mapping[str, Any]) -> str:
    rows: list[list[str]] = []
    for key, value in mapping.items():
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            rendered = ", ".join(str(item) for item in value)
        else:
            rendered = str(value)
        rows.append([_escape(str(key).replace("_", " ")), _escape(rendered)])
    if not rows:
        return ""
    return _output_html_block(title, _table_html(["Field", "Value"], rows))


def _metadata_outputs(payload: Mapping[str, Any]) -> list[Any]:
    sections = [
        ("Identity", payload.get("identity", {})),
        ("Access / Download", payload.get("access_download", {})),
        ("Subjects / Sessions", payload.get("subjects_sessions", {})),
        ("Modalities / Tasks", payload.get("modalities_tasks", {})),
        ("Standards / File Formats", payload.get("standards_file_formats", {})),
        ("Links / Publications", payload.get("links_publications", {})),
    ]
    html_sections = [
        _section_table_html(title, section if isinstance(section, Mapping) else {})
        for title, section in sections
    ]
    html_sections = [section for section in html_sections if section]
    if not html_sections:
        return []
    plain_lines = []
    for title, section in sections:
        if not isinstance(section, Mapping) or not section:
            continue
        plain_lines.append(title)
        for key, value in section.items():
            plain_lines.append(f"- {key}: {value}")
    return [
        _new_output(
            output_type="display_data",
            data={
                "text/plain": "\n".join(plain_lines),
                "text/html": "".join(html_sections),
                "application/json": dict(payload),
            },
        )
    ]


def _diagnostic_markdown(diagnostics: list[dict[str, Any]]) -> str:
    lines = ["## Notebook diagnostics", ""]
    for diag in diagnostics:
        status = _text(diag.get("status"), default="progress")
        line = _text(diag.get("human_line"))
        if line:
            lines.append(f"- **{status}**: {line}")
    return "\n".join(lines).strip()


def _dataset_catalog_payload_is_underspecified(payload: Mapping[str, Any] | None) -> bool:
    if not isinstance(payload, Mapping):
        return False
    datasets = [item for item in list(payload.get("datasets", []) or []) if isinstance(item, Mapping)]
    if not datasets:
        return False
    generic_rows = 0
    for row in datasets:
        label = _text(row.get("dataset_label"))
        source = _text(row.get("source"))
        accession = _text(row.get("accession"))
        dataset_id = _text(row.get("dataset_id"))
        doi = _catalog_doi(row)
        notes = _text(row.get("notes"))
        generic_label = label.casefold() in {"", "dataset"}
        has_identifier = bool(source or accession or dataset_id or doi)
        if generic_label and not has_identifier and not notes:
            generic_rows += 1
    return generic_rows == len(datasets)


def _renderable_dataset_comparison_cell_ids(
    cells: list[dict[str, Any]],
    artifacts: Mapping[str, Any],
) -> set[str]:
    latest_by_task: dict[str, str] = {}
    latest_global = ""
    for cell in cells:
        if _text(cell.get("type")) != "dataset_comparison":
            continue
        artifact = dict(artifacts.get(_text(cell.get("artifact_id"))) or {})
        payload = artifact.get("payload") if isinstance(artifact.get("payload"), Mapping) else {}
        if _dataset_catalog_payload_is_underspecified(payload):
            continue
        task_id = _text(cell.get("task_id"))
        cell_id = _text(cell.get("cell_id"))
        if task_id:
            latest_by_task[task_id] = cell_id
        if cell_id:
            latest_global = cell_id
    renderable = {cell_id for cell_id in latest_by_task.values() if cell_id}
    if not renderable and latest_global:
        renderable.add(latest_global)
    return renderable


def build_analysis_notebook(
    workspace: Mapping[str, Any] | None,
    *,
    conversation_id: str = "",
    title: str = "Open Data Analysis Notebook",
    diagnostics: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    ws = dict(workspace or {})
    artifacts = dict(ws.get("artifacts") or {})
    cells = list(ws.get("cells", []) or [])
    renderable_comparison_ids = _renderable_dataset_comparison_cell_ids(cells, artifacts)

    nb_cells: list[Any] = []
    intro_lines = [f"# {title}"]
    if conversation_id:
        intro_lines.append("")
        intro_lines.append(f"Conversation: `{conversation_id}`")
    selected_dataset_id = _text(ws.get("selected_dataset_id"))
    if selected_dataset_id:
        intro_lines.append(f"Selected dataset: `{selected_dataset_id}`")
    nb_cells.append(
        _new_markdown_cell(
            "\n".join(intro_lines).strip(),
            metadata={"co_scientist": {"kind": "notebook_header"}},
        )
    )

    for cell in cells:
        artifact = dict(artifacts.get(_text(cell.get("artifact_id"))) or {})
        cell_type = _text(cell.get("type"))
        payload = artifact.get("payload", {}) if isinstance(artifact.get("payload"), Mapping) else {}
        base_metadata = _metadata_base(cell=cell, artifact=artifact)
        if cell_type == "dataset_comparison":
            if _text(cell.get("cell_id")) not in renderable_comparison_ids:
                continue
            metadata = dict(base_metadata)
            metadata["co_scientist"]["kind"] = "dataset_comparison"
            metadata["co_scientist"]["dataset_rows"] = list(payload.get("datasets", []) or [])
            nb_cells.append(
                _new_code_cell(
                    source=_dataset_comparison_source(payload, artifact),
                    execution_count=None,
                    outputs=_dataset_comparison_outputs(payload),
                    metadata=metadata,
                )
            )
            continue
        if cell_type == "dataset_metadata":
            metadata = dict(base_metadata)
            metadata["co_scientist"]["kind"] = "dataset_metadata"
            metadata["co_scientist"]["dataset_id"] = _text(payload.get("dataset_id"))
            nb_cells.append(
                _new_code_cell(
                    source=_metadata_source(payload, artifact),
                    execution_count=None,
                    outputs=_metadata_outputs(payload),
                    metadata=metadata,
                )
            )
            continue
        if cell_type == "analysis_note":
            metadata = dict(base_metadata)
            metadata["co_scientist"]["kind"] = "analysis_note"
            markdown = _text(payload.get("markdown") or artifact.get("summary"))
            nb_cells.append(_new_markdown_cell(markdown, metadata=metadata))
            continue
        fallback_text = _text(artifact.get("summary"))
        if fallback_text:
            metadata = dict(base_metadata)
            metadata["co_scientist"]["kind"] = "artifact_summary"
            nb_cells.append(_new_markdown_cell(fallback_text, metadata=metadata))

    diag_rows = [dict(item) for item in list(diagnostics or []) if isinstance(item, Mapping)]
    if diag_rows:
        nb_cells.append(
            _new_markdown_cell(
                _diagnostic_markdown(diag_rows),
                metadata={"co_scientist": {"kind": "diagnostics"}},
            )
        )

    notebook = _new_notebook(
        cells=nb_cells,
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {
                "name": "python",
                "file_extension": ".py",
                "mimetype": "text/x-python",
            },
            "co_scientist": {
                "schema": ANALYSIS_NOTEBOOK_SCHEMA,
                "conversation_id": conversation_id,
                "workspace_revision": int(ws.get("revision", 0) or 0),
                "selected_dataset_id": selected_dataset_id,
                "generated_at": _utc_now(),
            },
        },
    )
    return {
        "schema": ANALYSIS_NOTEBOOK_SCHEMA,
        "generated_at": _utc_now(),
        "conversation_id": conversation_id,
        "workspace_revision": int(ws.get("revision", 0) or 0),
        "selected_dataset_id": selected_dataset_id,
        "cell_count": len(nb_cells),
        "notebook": notebook,
    }


def notebook_json_for_api(payload: Mapping[str, Any]) -> dict[str, Any]:
    notebook = payload.get("notebook")
    return {
        "schema": _text(payload.get("schema"), default=ANALYSIS_NOTEBOOK_SCHEMA),
        "generated_at": _text(payload.get("generated_at")),
        "conversation_id": _text(payload.get("conversation_id")),
        "workspace_revision": int(payload.get("workspace_revision", 0) or 0),
        "selected_dataset_id": _text(payload.get("selected_dataset_id")),
        "cell_count": int(payload.get("cell_count", 0) or 0),
        "notebook": dict(notebook or {}),
    }


def serialize_notebook_ipynb(payload: Mapping[str, Any]) -> str:
    notebook = payload.get("notebook")
    if isinstance(notebook, Mapping) and writes is not None and from_dict is not None:
        return writes(from_dict(dict(notebook)))
    return json.dumps(
        dict(notebook or _new_notebook(cells=[], metadata={})),
        indent=2,
        ensure_ascii=True,
        sort_keys=False,
    )


__all__ = [
    "ANALYSIS_NOTEBOOK_SCHEMA",
    "build_analysis_notebook",
    "notebook_json_for_api",
    "serialize_notebook_ipynb",
]
