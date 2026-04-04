"""Build a real nbformat notebook from the analysis workspace."""

from __future__ import annotations

import ast
import base64
import contextlib
from datetime import datetime, timezone
import html
import importlib
import io
import json
import sys
import traceback
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

try:  # pragma: no cover - dependency can be absent in some runtimes
    import pandas as pd
except ImportError:  # pragma: no cover
    pd = None

try:  # pragma: no cover - dependency can be absent in some runtimes
    import numpy as np
except ImportError:  # pragma: no cover
    np = None

matplotlib = None
plt = None
MatplotlibAxes = None
MatplotlibFigure = None
sns = None


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


_ALLOWED_NOTEBOOK_IMPORTS = {
    "collections",
    "itertools",
    "json",
    "math",
    "matplotlib",
    "numpy",
    "pandas",
    "seaborn",
    "statistics",
}
_BANNED_NOTEBOOK_CALLS = {
    "__import__",
    "compile",
    "eval",
    "exec",
    "globals",
    "help",
    "input",
    "locals",
    "open",
    "quit",
}
_LIST_LIKE_NOTEBOOK_COLUMNS = ("modalities", "diagnoses", "tasks", "tags", "evidence_ids")


def _runtime_dependency_failure_message(module_name: str, exc: BaseException) -> str:
    version = str(sys.version.split()[0] if sys.version else "").strip()
    executable = str(sys.executable or "").strip()
    return (
        f"{module_name} is not available in this runtime "
        f"(python={executable or 'unknown'}, version={version or 'unknown'}). "
        f"Original error: {type(exc).__name__}: {exc}"
    )


def _ensure_matplotlib_runtime() -> None:
    global matplotlib, plt, MatplotlibAxes, MatplotlibFigure
    if (
        matplotlib is not None
        and plt is not None
        and MatplotlibAxes is not None
        and MatplotlibFigure is not None
    ):
        return
    try:
        module = importlib.import_module("matplotlib")
        with contextlib.suppress(Exception):
            module.use("Agg", force=True)
        pyplot_module = importlib.import_module("matplotlib.pyplot")
        axes_module = importlib.import_module("matplotlib.axes")
        figure_module = importlib.import_module("matplotlib.figure")
    except Exception as exc:  # pragma: no cover - depends on user runtime
        matplotlib = None
        plt = None
        MatplotlibAxes = None
        MatplotlibFigure = None
        raise ImportError(_runtime_dependency_failure_message("matplotlib", exc)) from exc
    matplotlib = module
    plt = pyplot_module
    MatplotlibAxes = getattr(axes_module, "Axes", None)
    MatplotlibFigure = getattr(figure_module, "Figure", None)


def _ensure_seaborn_runtime() -> None:
    global sns
    if sns is not None:
        return
    try:
        _ensure_matplotlib_runtime()
        sns = importlib.import_module("seaborn")
    except Exception as exc:  # pragma: no cover - depends on user runtime
        sns = None
        raise ImportError(_runtime_dependency_failure_message("seaborn", exc)) from exc


def notebook_runtime_status() -> dict[str, Any]:
    def _module_status(module_name: str) -> dict[str, Any]:
        try:
            module = importlib.import_module(module_name)
            return {
                "available": True,
                "version": str(getattr(module, "__version__", "") or ""),
                "error": "",
            }
        except Exception as exc:  # pragma: no cover - depends on user runtime
            return {
                "available": False,
                "version": "",
                "error": f"{type(exc).__name__}: {exc}",
            }

    return {
        "python_executable": str(sys.executable or "").strip(),
        "python_version": str(sys.version.split()[0] if sys.version else "").strip(),
        "pandas": _module_status("pandas"),
        "numpy": _module_status("numpy"),
        "matplotlib": _module_status("matplotlib"),
        "seaborn": _module_status("seaborn"),
    }


def _requested_notebook_runtime_modules(tree: ast.AST) -> set[str]:
    requested: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = str(alias.name or "").split(".", 1)[0]
                if root in {"matplotlib", "seaborn"}:
                    requested.add(root)
        elif isinstance(node, ast.ImportFrom):
            root = str(node.module or "").split(".", 1)[0]
            if root in {"matplotlib", "seaborn"}:
                requested.add(root)
        elif isinstance(node, ast.Name):
            if node.id in {"plt", "display_figure"}:
                requested.add("matplotlib")
            elif node.id == "sns":
                requested.add("seaborn")
    return requested


def _safe_notebook_import(name: str, globals_: Any = None, locals_: Any = None, fromlist: tuple[str, ...] = (), level: int = 0) -> Any:
    root = str(name or "").split(".", 1)[0]
    if root not in _ALLOWED_NOTEBOOK_IMPORTS:
        raise ImportError(f"Import '{name}' is not allowed in notebook cells.")
    if root == "pandas" and pd is None:
        raise ImportError("pandas is not available in this runtime.")
    if root == "numpy" and np is None:
        raise ImportError("numpy is not available in this runtime.")
    if root == "matplotlib":
        _ensure_matplotlib_runtime()
    if root == "seaborn":
        _ensure_seaborn_runtime()
    return __import__(name, globals_, locals_, fromlist, level)


def _validate_agent_notebook_source(source: str) -> ast.Module:
    try:
        tree = ast.parse(source, mode="exec")
    except SyntaxError as exc:
        raise ValueError(f"Notebook cell code has invalid Python syntax: {exc.msg}.") from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = str(alias.name or "").split(".", 1)[0]
                if root not in _ALLOWED_NOTEBOOK_IMPORTS:
                    raise ValueError(f"Notebook imports must be limited to: {', '.join(sorted(_ALLOWED_NOTEBOOK_IMPORTS))}.")
        elif isinstance(node, ast.ImportFrom):
            root = str(node.module or "").split(".", 1)[0]
            if root not in _ALLOWED_NOTEBOOK_IMPORTS:
                raise ValueError(f"Notebook imports must be limited to: {', '.join(sorted(_ALLOWED_NOTEBOOK_IMPORTS))}.")
        elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if str(node.func.id or "") in _BANNED_NOTEBOOK_CALLS:
                raise ValueError(f"Notebook code cannot call `{node.func.id}`.")
        elif isinstance(node, ast.Attribute):
            if str(node.attr or "").startswith("__"):
                raise ValueError("Notebook code cannot access dunder attributes.")
    return tree


def _safe_notebook_builtins(print_fn: Any) -> dict[str, Any]:
    return {
        "__import__": _safe_notebook_import,
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "Exception": Exception,
        "filter": filter,
        "float": float,
        "int": int,
        "isinstance": isinstance,
        "len": len,
        "list": list,
        "map": map,
        "max": max,
        "min": min,
        "next": next,
        "print": print_fn,
        "range": range,
        "reversed": reversed,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "type": type,
        "zip": zip,
    }


def _normalize_list_like_columns(frame: Any) -> Any:
    if pd is None or not isinstance(frame, pd.DataFrame) or frame.empty:
        return frame
    output = frame.copy()
    for column in _LIST_LIKE_NOTEBOOK_COLUMNS:
        if column not in output.columns:
            continue
        output[column] = output[column].apply(
            lambda values: ", ".join(str(item).strip() for item in values if str(item).strip())
            if isinstance(values, (list, tuple, set))
            else values
        )
    return output


def _truncate_frame(frame: Any, *, max_rows: int = 30) -> tuple[Any, str]:
    if pd is None or not isinstance(frame, pd.DataFrame):
        return frame, ""
    if len(frame.index) <= max_rows:
        return frame, ""
    return frame.head(max_rows), f"Showing first {max_rows} of {len(frame.index)} rows."


def _render_dataframe_output(frame: Any) -> dict[str, Any]:
    if pd is None or not isinstance(frame, pd.DataFrame):
        return _new_output(output_type="display_data", data={"text/plain": _text(frame)})
    normalized = _normalize_list_like_columns(frame)
    truncated, note = _truncate_frame(normalized)
    plain = truncated.to_string(index=False)
    html_value = truncated.to_html(index=False, classes=["dataframe"], border=0)
    if note:
        plain = f"{note}\n{plain}"
        html_value = f"<p><em>{_escape(note)}</em></p>{html_value}"
    return _new_output(
        output_type="display_data",
        data={
            "text/plain": plain,
            "text/html": html_value,
        },
    )


def _render_series_output(series: Any) -> dict[str, Any]:
    if pd is None or not isinstance(series, pd.Series):
        return _new_output(output_type="display_data", data={"text/plain": _text(series)})
    frame = series.reset_index()
    frame.columns = ["index", series.name or "value"]
    return _render_dataframe_output(frame)


def _render_json_output(value: Any) -> dict[str, Any]:
    return _new_output(
        output_type="display_data",
        data={
            "text/plain": _pretty_json(value),
            "application/json": value,
        },
    )


def _render_svg_output(svg: str, *, title: str = "Notebook figure") -> dict[str, Any]:
    return _new_output(
        output_type="display_data",
        data={
            "image/svg+xml": svg,
            "text/plain": title,
        },
    )


def _render_png_output(png_base64: str, *, title: str = "Notebook figure") -> dict[str, Any]:
    return _new_output(
        output_type="display_data",
        data={
            "image/png": png_base64,
            "text/plain": title,
        },
    )


def _matplotlib_figure_title(figure: Any, *, default: str = "Notebook figure") -> str:
    if MatplotlibFigure is None or not isinstance(figure, MatplotlibFigure):
        return default
    suptitle = getattr(figure, "_suptitle", None)
    if suptitle is not None:
        text = _text(getattr(suptitle, "get_text", lambda: "")())
        if text:
            return text
    for axis in list(getattr(figure, "axes", []) or []):
        title = _text(getattr(axis, "get_title", lambda: "")())
        if title:
            return title
    return default


def _render_matplotlib_figure_output(figure: Any, *, default_title: str = "Notebook figure") -> dict[str, Any]:
    if MatplotlibFigure is None or not isinstance(figure, MatplotlibFigure):
        return _new_output(output_type="display_data", data={"text/plain": _text(figure)})
    title = _matplotlib_figure_title(figure, default=default_title)
    svg_buffer = io.StringIO()
    with contextlib.suppress(Exception):
        figure.savefig(svg_buffer, format="svg", bbox_inches="tight")
        svg_value = svg_buffer.getvalue().strip()
        if svg_value:
            return _render_svg_output(svg_value, title=title)
    png_buffer = io.BytesIO()
    figure.savefig(png_buffer, format="png", bbox_inches="tight", dpi=144)
    png_base64 = base64.b64encode(png_buffer.getvalue()).decode("ascii")
    return _render_png_output(png_base64, title=title)


def _output_summary_label(output: Mapping[str, Any]) -> str:
    output_type = _text(output.get("output_type"))
    if output_type == "stream":
        return "stdout"
    if output_type == "error":
        return "error"
    data = output.get("data") if isinstance(output.get("data"), Mapping) else {}
    if "image/svg+xml" in data:
        return _text(data.get("text/plain"), default="svg figure")
    if "image/png" in data:
        return _text(data.get("text/plain"), default="figure")
    if "text/html" in data:
        html_value = _text(data.get("text/html"))
        if "dataframe" in html_value:
            return "table"
        return "rich output"
    if "application/json" in data:
        return "json"
    return _text(data.get("text/plain"), default="text output").splitlines()[0][:72] or "text output"


def summarize_notebook_outputs(outputs: list[Mapping[str, Any]] | None) -> list[str]:
    summaries: list[str] = []
    for output in list(outputs or []):
        summary = _output_summary_label(output)
        if summary and summary not in summaries:
            summaries.append(summary)
    return summaries[:8]


def _frame_records(frame: Any) -> list[dict[str, Any]]:
    if pd is not None and isinstance(frame, pd.DataFrame):
        records = frame.to_dict(orient="records")
        return [dict(record or {}) for record in records]
    if isinstance(frame, list):
        return [dict(item or {}) for item in frame if isinstance(item, Mapping)]
    return []


def _multivalue_items(value: Any) -> list[str]:
    raw_values: list[Any]
    if isinstance(value, (list, tuple, set)):
        raw_values = list(value)
    else:
        text = str(value or "").strip()
        if not text:
            return []
        raw_values = [part.strip() for part in text.split(",")] if "," in text else [text]
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in raw_values:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def multivalue_long_df(frame: Any, index_col: str, column: str) -> Any:
    if pd is None:
        raise ValueError("pandas is required for notebook dataframe helpers.")
    if not isinstance(frame, pd.DataFrame) or index_col not in frame.columns or column not in frame.columns:
        return pd.DataFrame(columns=[index_col, column])
    rows: list[dict[str, Any]] = []
    for _, row in frame[[index_col, column]].iterrows():
        label = row.get(index_col)
        for item in _multivalue_items(row.get(column)):
            rows.append({index_col: label, column: item})
    if not rows:
        return pd.DataFrame(columns=[index_col, column])
    return pd.DataFrame(rows, columns=[index_col, column]).reset_index(drop=True)


def multivalue_presence_df(frame: Any, index_col: str, column: str) -> Any:
    if pd is None:
        raise ValueError("pandas is required for notebook dataframe helpers.")
    long_df = multivalue_long_df(frame, index_col, column)
    if not isinstance(long_df, pd.DataFrame) or long_df.empty:
        return pd.DataFrame(columns=[index_col])
    matrix = (
        long_df.assign(value=1)
        .pivot_table(index=index_col, columns=column, values="value", aggfunc="max", fill_value=0)
        .sort_index(axis=1)
        .reset_index()
    )
    matrix.columns = [str(item or "").strip() for item in list(matrix.columns)]
    return matrix


def explode_multivalue_counts(frame: Any, column: str, *, limit: int = 20) -> Any:
    if pd is None:
        raise ValueError("pandas is required for notebook dataframe helpers.")
    if not isinstance(frame, pd.DataFrame) or column not in frame.columns:
        return pd.DataFrame(columns=["value", "count"])
    values: list[str] = []
    for item in frame[column].tolist():
        values.extend(_multivalue_items(item))
    if not values:
        return pd.DataFrame(columns=["value", "count"])
    counts = (
        pd.Series(values, dtype="string")
        .value_counts()
        .rename_axis("value")
        .reset_index(name="count")
    )
    return counts.head(max(1, int(limit or 20)))


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
    summary_cards = payload.get("summary_cards", {}) if isinstance(payload.get("summary_cards"), Mapping) else {}
    summary_text = _text(payload.get("summary") or artifact.get("summary"))
    dimension_labels = [
        _dimension_label(item)
        for item in list(payload.get("dimensions", []) or [])
        if _dimension_label(item)
    ]
    return (
        "import pandas as pd\n"
        "from IPython.display import display\n\n"
        "# `catalog_payload` is reconstructed from the stored dataset comparison artifact.\n"
        f"# Summary: {summary_text or 'Stored dataset comparison'}\n"
        f"# Datasets: {int(summary_cards.get('dataset_count', 0) or 0)} | Sources: {int(summary_cards.get('source_count', 0) or 0)}\n"
        f"# Comparison axes: {', '.join(dimension_labels) if dimension_labels else 'metadata overview'}\n\n"
        "comparison_df = pd.DataFrame(catalog_payload['datasets'])\n"
        "for column in ['modalities', 'diagnoses', 'tasks']:\n"
        "    if column in comparison_df.columns:\n"
        "        comparison_df[column] = comparison_df[column].apply(\n"
        "            lambda values: ', '.join(values) if isinstance(values, list) else values\n"
        "        )\n\n"
        "display_columns = [\n"
        "    'accession', 'source', 'participant_count', 'session_count',\n"
        "    'modalities', 'diagnoses', 'tasks', 'doi', 'bids'\n"
        "]\n"
        "display(\n"
        "    comparison_df[[column for column in display_columns if column in comparison_df.columns]]\n"
        "    .sort_values(['participant_count', 'accession'], ascending=[False, True], na_position='last')\n"
        ")\n"
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


def _count_chart_svg_from_counts(counts: Mapping[str, int], *, title: str, color: str) -> str:
    if not counts:
        return ""
    sorted_counts = sorted(
        ((str(label).strip(), int(value or 0)) for label, value in counts.items() if str(label).strip() and int(value or 0) > 0),
        key=lambda item: (-item[1], item[0]),
    )[:12]
    if not sorted_counts:
        return ""
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


def bar_chart_svg(
    frame: Any,
    *,
    label_col: str,
    value_col: str,
    title: str,
    color: str = "#4C78A8",
    limit: int = 20,
    sort_desc: bool = True,
) -> str:
    rows = _frame_records(frame)
    trimmed: list[dict[str, Any]] = []
    for row in rows:
        label = _text(row.get(label_col))
        value = row.get(value_col)
        if not label or value in (None, "", [], {}):
            continue
        candidate = dict(row)
        candidate["accession"] = label
        candidate[value_col] = value
        trimmed.append(candidate)
    def _sort_key(row: Mapping[str, Any]) -> tuple[float, str]:
        try:
            numeric = float(row.get(value_col))
        except (TypeError, ValueError):
            numeric = float("-inf")
        return (numeric, _text(row.get("accession")))
    trimmed = sorted(trimmed, key=_sort_key, reverse=bool(sort_desc))[: max(1, int(limit or 20))]
    return _numeric_chart_svg(trimmed, key=value_col, title=title, color=color)


def count_chart_svg(
    frame_or_counts: Any,
    *,
    column: str = "",
    title: str,
    color: str = "#72B7B2",
    limit: int = 12,
) -> str:
    counts: dict[str, int] = {}
    if pd is not None and isinstance(frame_or_counts, pd.DataFrame):
        if not column or column not in frame_or_counts.columns:
            return ""
        counts_frame = explode_multivalue_counts(frame_or_counts, column, limit=limit)
        for _, row in counts_frame.iterrows():
            label = _text(row.get("value"))
            try:
                value = int(row.get("count") or 0)
            except (TypeError, ValueError):
                value = 0
            if label and value > 0:
                counts[label] = value
    elif isinstance(frame_or_counts, Mapping):
        for key, value in frame_or_counts.items():
            label = _text(key)
            try:
                count = int(value or 0)
            except (TypeError, ValueError):
                count = 0
            if label and count > 0:
                counts[label] = count
    else:
        for item in _list_text(frame_or_counts):
            counts[item] = counts.get(item, 0) + 1
    return _count_chart_svg_from_counts(counts, title=title, color=color)


def multivalue_matrix_svg(
    frame: Any,
    *,
    column: str,
    title: str,
    row_label_col: str = "accession",
    color: str = "#0ea5b7",
    limit: int = NOTEBOOK_COMPARISON_MAX_DATASETS,
    max_columns: int = 10,
) -> str:
    rows = _frame_records(frame)[: max(1, int(limit or NOTEBOOK_COMPARISON_MAX_DATASETS))]
    if not rows:
        return ""
    labels: list[str] = []
    row_labels: list[str] = []
    values: list[list[int]] = []
    for row in rows:
        row_labels.append(_text(row.get(row_label_col) or row.get("dataset_label"), default="Dataset"))
        items = set(_list_text(row.get(column)))
        for item in items:
            if item not in labels:
                labels.append(item)
        labels = labels[:max_columns]
    if not labels:
        return ""
    for row in rows:
        items = set(_list_text(row.get(column)))
        values.append([1 if label in items else 0 for label in labels])
    return _binary_matrix_svg(
        row_labels=row_labels,
        column_labels=labels,
        values=values,
        title=title,
        on_color=color,
    )


def presence_matrix_svg(
    frame: Any,
    *,
    fields: list[str] | list[tuple[str, str]],
    title: str,
    row_label_col: str = "accession",
    limit: int = NOTEBOOK_COMPARISON_MAX_DATASETS,
) -> str:
    rows = _frame_records(frame)[: max(1, int(limit or NOTEBOOK_COMPARISON_MAX_DATASETS))]
    if not rows:
        return ""
    normalized_fields: list[tuple[str, str]] = []
    for item in fields:
        if isinstance(item, tuple):
            normalized_fields.append((_text(item[0]), _text(item[1]) or _text(item[0]).replace("_", " ").title()))
        else:
            key = _text(item)
            normalized_fields.append((key, key.replace("_", " ").title()))
    row_labels = [_text(row.get(row_label_col) or row.get("dataset_label"), default="Dataset") for row in rows]
    matrix_values: list[list[int]] = []
    for row in rows:
        matrix_values.append([
            0 if row.get(key) in (None, "", [], {}) else 1
            for key, _label in normalized_fields
        ])
    return _binary_matrix_svg(
        row_labels=row_labels,
        column_labels=[label for _, label in normalized_fields],
        values=matrix_values,
        title=title,
        on_color="#2563eb",
    )


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


class _NotebookOutputCollector:
    def __init__(self) -> None:
        self.outputs: list[dict[str, Any]] = []

    def add_output(self, output: Mapping[str, Any]) -> None:
        self.outputs.append(dict(output))

    def display(self, *values: Any) -> None:
        for value in values:
            if MatplotlibAxes is not None and isinstance(value, MatplotlibAxes):
                self.add_output(_render_matplotlib_figure_output(value.figure))
                if plt is not None:
                    with contextlib.suppress(Exception):
                        plt.close(value.figure)
                continue
            if MatplotlibFigure is not None and isinstance(value, MatplotlibFigure):
                self.add_output(_render_matplotlib_figure_output(value))
                if plt is not None:
                    with contextlib.suppress(Exception):
                        plt.close(value)
                continue
            if pd is not None and isinstance(value, pd.DataFrame):
                self.add_output(_render_dataframe_output(value))
                continue
            if pd is not None and isinstance(value, pd.Series):
                self.add_output(_render_series_output(value))
                continue
            if isinstance(value, Mapping):
                self.add_output(_render_json_output(dict(value)))
                continue
            if isinstance(value, (list, tuple)) and value and all(isinstance(item, Mapping) for item in value):
                self.add_output(_render_dataframe_output(pd.DataFrame(list(value))) if pd is not None else _render_json_output(list(value)))
                continue
            if isinstance(value, str) and value.lstrip().startswith("<svg"):
                self.add_output(_render_svg_output(value))
                continue
            self.add_output(
                _new_output(
                    output_type="display_data",
                    data={"text/plain": _text(value)},
                )
            )

    def display_svg(self, svg: str, *, title: str = "Notebook figure") -> None:
        clean = str(svg or "").strip()
        if not clean:
            return
        self.add_output(_render_svg_output(clean, title=title))

    def display_figure(self, figure: Any, *, title: str = "Notebook figure") -> None:
        self.add_output(_render_matplotlib_figure_output(figure, default_title=title))
        if plt is not None and MatplotlibFigure is not None and isinstance(figure, MatplotlibFigure):
            with contextlib.suppress(Exception):
                plt.close(figure)

    def append_stdout(self, text: str) -> None:
        clean = str(text or "")
        if not clean.strip():
            return
        self.add_output(
            _new_output(
                output_type="stream",
                name="stdout",
                text=clean,
            )
        )


def execute_agent_notebook_cell(
    *,
    source_code: str,
    catalog_payload: Mapping[str, Any] | None = None,
    dataset_profile: Mapping[str, Any] | None = None,
    analysis_request: Mapping[str, Any] | None = None,
    catalog_profile: Mapping[str, Any] | None = None,
    analysis_context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    source = str(source_code or "").strip()
    if not source:
        raise ValueError("source_code must not be empty.")
    tree = _validate_agent_notebook_source(source)
    required_runtime_modules = _requested_notebook_runtime_modules(tree)
    if "matplotlib" in required_runtime_modules:
        try:
            _ensure_matplotlib_runtime()
        except ImportError as exc:
            raise ValueError(f"Notebook cell execution failed: {exc}") from exc
    if "seaborn" in required_runtime_modules:
        try:
            _ensure_seaborn_runtime()
        except ImportError as exc:
            raise ValueError(f"Notebook cell execution failed: {exc}") from exc

    comparison_df = (
        pd.DataFrame(list((catalog_payload or {}).get("datasets", []) or []))
        if pd is not None and isinstance(catalog_payload, Mapping)
        else None
    )
    collector = _NotebookOutputCollector()
    stdout_buffer = io.StringIO()
    original_show = None

    def _capture_open_matplotlib_figures() -> None:
        if plt is None:
            return
        figure_numbers = list(plt.get_fignums())
        for index, figure_number in enumerate(figure_numbers, start=1):
            figure = plt.figure(figure_number)
            collector.display_figure(figure, title=f"Figure {index}")
        if figure_numbers:
            plt.close("all")

    env: dict[str, Any] = {
        "__builtins__": _safe_notebook_builtins(print),
        "metadata_snapshot": dict(catalog_payload or {}),
        "catalog_payload": dict(catalog_payload or {}),
        "catalog_profile": dict(catalog_profile or {}),
        "analysis_request": dict(analysis_request or {}),
        "analysis_context": dict(analysis_context or {}),
        "catalog_df": comparison_df.copy() if pd is not None and isinstance(comparison_df, pd.DataFrame) else comparison_df,
        "dataset_profile": dict(dataset_profile or {}),
        "dataset_metadata": dict(dataset_profile or {}),
        "comparison_df": comparison_df.copy() if pd is not None and isinstance(comparison_df, pd.DataFrame) else comparison_df,
        "display": collector.display,
        "display_svg": collector.display_svg,
        "display_figure": collector.display_figure,
        "explode_multivalue_counts": explode_multivalue_counts,
        "multivalue_long_df": multivalue_long_df,
        "multivalue_presence_df": multivalue_presence_df,
        "bar_chart_svg": bar_chart_svg,
        "count_chart_svg": count_chart_svg,
        "multivalue_matrix_svg": multivalue_matrix_svg,
        "presence_matrix_svg": presence_matrix_svg,
        "json": json,
    }
    if pd is not None:
        env["pd"] = pd
    if np is not None:
        env["np"] = np
    if matplotlib is None:
        with contextlib.suppress(ImportError):
            _ensure_matplotlib_runtime()
    if sns is None:
        with contextlib.suppress(ImportError):
            _ensure_seaborn_runtime()
    if matplotlib is not None:
        env["matplotlib"] = matplotlib
    if plt is not None:
        def _captured_show(*args: Any, **kwargs: Any) -> None:
            del args, kwargs
            _capture_open_matplotlib_figures()

        original_show = plt.show
        plt.show = _captured_show
        env["plt"] = plt
    if sns is not None:
        env["sns"] = sns
    def _captured_print(*args: Any, **kwargs: Any) -> None:
        forwarded = dict(kwargs)
        forwarded["file"] = stdout_buffer
        print(*args, **forwarded)
    env["print"] = _captured_print
    env["__builtins__"]["print"] = env["print"]

    last_expr_value: Any = None
    body_nodes = list(tree.body)
    last_expr = body_nodes[-1] if body_nodes and isinstance(body_nodes[-1], ast.Expr) else None
    if last_expr is not None:
        body_nodes = body_nodes[:-1]
    try:
        if body_nodes:
            module = ast.Module(body=body_nodes, type_ignores=[])
            compiled = compile(module, "<analysis_notebook_cell>", "exec")
            with contextlib.redirect_stdout(stdout_buffer):
                exec(compiled, env, env)
        if last_expr is not None:
            expression = ast.Expression(last_expr.value)
            compiled_expr = compile(expression, "<analysis_notebook_cell>", "eval")
            with contextlib.redirect_stdout(stdout_buffer):
                last_expr_value = eval(compiled_expr, env, env)
        if last_expr_value is not None:
            collector.display(last_expr_value)
        _capture_open_matplotlib_figures()
    except Exception as exc:
        message = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        raise ValueError(f"Notebook cell execution failed: {message}") from exc
    finally:
        if plt is not None and original_show is not None:
            plt.show = original_show
            with contextlib.suppress(Exception):
                plt.close("all")

    collector.append_stdout(stdout_buffer.getvalue())
    outputs = list(collector.outputs)
    return {
        "outputs": outputs,
        "output_summary": summarize_notebook_outputs(outputs),
        "context": {
            "has_catalog_payload": bool(isinstance(catalog_payload, Mapping) and dict(catalog_payload or {})),
            "has_dataset_profile": bool(isinstance(dataset_profile, Mapping) and dict(dataset_profile or {})),
            "dataset_count": int(len(comparison_df.index)) if pd is not None and isinstance(comparison_df, pd.DataFrame) else 0,
        },
    }


def _dataset_stat_pairs(payload: Mapping[str, Any], records: list[dict[str, Any]]) -> list[tuple[str, str]]:
    summary_cards = payload.get("summary_cards", {}) if isinstance(payload.get("summary_cards"), Mapping) else {}
    dimensions = [
        _dimension_label(item)
        for item in list(payload.get("dimensions", []) or [])
        if _dimension_label(item)
    ]
    known_participant_counts = sum(1 for row in records if row.get("participant_count") not in (None, "", [], {}))
    return [
        ("Datasets", str(int(summary_cards.get("dataset_count", 0) or len(records) or 0))),
        ("Sources", str(int(summary_cards.get("source_count", 0) or 0))),
        ("Comparison axes", ", ".join(dimensions[:4]) if dimensions else "Metadata overview"),
        ("Rows with participant counts", str(known_participant_counts)),
    ]


def _dataset_comparison_overview_text(payload: Mapping[str, Any], records: list[dict[str, Any]]) -> str:
    summary = _text(payload.get("summary"))
    labels = [
        _text(record.get("accession") or record.get("dataset_label"))
        for record in records[:6]
        if _text(record.get("accession") or record.get("dataset_label"))
    ]
    parts = [summary] if summary else []
    if labels:
        parts.append(f"Datasets: {', '.join(labels)}")
    return "\n".join(part for part in parts if part).strip()


def _dataset_comparison_overview_html(payload: Mapping[str, Any], records: list[dict[str, Any]]) -> str:
    summary = _text(payload.get("summary"))
    stat_rows = [[_escape(label), _escape(value)] for label, value in _dataset_stat_pairs(payload, records)]
    dataset_rows: list[list[str]] = []
    for record in records[:12]:
        dataset_name = _text(record.get("dataset_label") or record.get("accession"), default="Dataset")
        source_ref = " • ".join(
            part for part in (_text(record.get("source")), _text(record.get("accession"))) if part
        ) or "n/a"
        participants = _text(record.get("participant_count"), default="n/a")
        modalities = ", ".join(_list_text(record.get("modalities"))) or "n/a"
        tasks = ", ".join(_list_text(record.get("tasks"))) or ", ".join(_list_text(record.get("diagnoses"))) or "n/a"
        dataset_rows.append(
            [
                _escape(dataset_name),
                _escape(source_ref),
                _escape(participants),
                _escape(modalities),
                _escape(tasks),
            ]
        )
    body = ""
    if summary:
        body += f"<p><strong>{_escape(summary)}</strong></p>"
    body += _table_html(["Metric", "Value"], stat_rows)
    if dataset_rows:
        body += "<div style='height:12px'></div>"
        body += _table_html(["Dataset", "Source", "Participants", "Modalities", "Tasks / Diagnoses"], dataset_rows)
    return body


def _dataset_comparison_outputs(payload: Mapping[str, Any]) -> list[Any]:
    records = [_catalog_record(dataset) for dataset in list(payload.get("datasets", []) or [])[:NOTEBOOK_COMPARISON_MAX_DATASETS]]
    if not records:
        return []
    outputs = [
        _new_output(
            output_type="display_data",
            data={
                "text/plain": _dataset_comparison_overview_text(payload, records),
                "text/html": _dataset_comparison_overview_html(payload, records),
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
    summary_text = _text(payload.get("summary") or artifact.get("summary"))
    return (
        "import pandas as pd\n"
        "from IPython.display import display\n\n"
        "# `dataset_profile` is reconstructed from the stored metadata artifact.\n"
        f"# Summary: {summary_text or 'Stored dataset metadata profile'}\n\n"
        "dataset_metadata = dataset_profile\n"
        "section_names = [\n"
        "    'identity', 'access_download', 'subjects_sessions',\n"
        "    'modalities_tasks', 'standards_file_formats', 'links_publications'\n"
        "]\n"
        "for section_name in section_names:\n"
        "    section = dataset_profile.get(section_name, {})\n"
        "    if section:\n"
        "        display(pd.Series(section, name=section_name))\n"
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
        metrics = dict(diag.get("metrics") or {}) if isinstance(diag.get("metrics"), Mapping) else {}
        if line:
            lines.append(f"- **{status}**: {line}")
        required_tool_calls = [
            _text(item)
            for item in list(metrics.get("required_tool_calls", []) or [])
            if _text(item)
        ]
        if required_tool_calls:
            lines.append("  Required tool calls: " + ", ".join(f"`{item}`" for item in required_tool_calls))
        error_text = _text(metrics.get("error"))
        if error_text:
            lines.append(f"  Error: `{error_text}`")
        repair_hint = _text(metrics.get("repair_hint"))
        if repair_hint:
            lines.append(f"  Next step: {repair_hint}")
        source_preview = _text(metrics.get("source_preview"))
        if source_preview:
            lines.append(f"  Code preview: `{source_preview}`")
        response_preview = _text(metrics.get("response_preview"))
        if response_preview:
            lines.append(f"  Model returned: `{response_preview}`")
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


def _task_ids_with_rich_cells(
    cells: list[dict[str, Any]],
    *,
    renderable_comparison_ids: set[str],
) -> set[str]:
    task_ids: set[str] = set()
    for cell in cells:
        task_id = _text(cell.get("task_id"))
        if not task_id:
            continue
        cell_type = _text(cell.get("type"))
        if cell_type == "analysis_code":
            task_ids.add(task_id)
            continue
        if cell_type == "dataset_metadata":
            task_ids.add(task_id)
            continue
        if cell_type == "dataset_comparison" and _text(cell.get("cell_id")) in renderable_comparison_ids:
            task_ids.add(task_id)
    return task_ids


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

    nb_cells: list[Any] = []
    intro_lines = [f"# {title}"]
    if conversation_id:
        intro_lines.append("")
        intro_lines.append(f"Conversation: `{conversation_id}`")
    selected_dataset_id = _text(ws.get("selected_dataset_id"))
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
        task_id = _text(cell.get("task_id"))
        if cell_type == "analysis_code":
            metadata = dict(base_metadata)
            metadata["co_scientist"]["kind"] = _text(payload.get("kind"), default="analysis_code")
            metadata["co_scientist"]["output_summary"] = list(payload.get("output_summary", []) or [])
            nb_cells.append(
                _new_code_cell(
                    source=_text(payload.get("source_code")),
                    execution_count=None,
                    outputs=[dict(item) for item in list(payload.get("outputs", []) or []) if isinstance(item, Mapping)],
                    metadata=metadata,
                )
            )
            continue
        if cell_type in {"dataset_comparison", "dataset_metadata", "metadata_snapshot"}:
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

    diag_rows = [
        dict(item)
        for item in list(diagnostics or [])
        if isinstance(item, Mapping) and _text(item.get("status"), default="progress") == "error"
    ]
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
    "execute_agent_notebook_cell",
    "notebook_runtime_status",
    "notebook_json_for_api",
    "summarize_notebook_outputs",
    "serialize_notebook_ipynb",
]
