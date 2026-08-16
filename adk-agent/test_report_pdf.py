from pathlib import Path

import pytest

reportlab = pytest.importorskip("reportlab")
pytest.importorskip("markdown")
from reportlab.platypus import ListFlowable

import report_pdf


def test_extract_list_items_preserves_two_space_nesting():
    markdown = "- Parent\n  - Child\n    - Grandchild\n- Sibling\n"

    items, idx = report_pdf._extract_list_items(markdown.splitlines(), 0)

    assert idx == 4
    assert [item["level"] for item in items] == [0, 1, 2, 0]
    assert [item["text"] for item in items] == ["Parent", "Child", "Grandchild", "Sibling"]


def test_extract_list_items_restarts_ordered_sequence_after_unordered_break():
    markdown = "1. First\n- Divider\n1. Restarted\n"

    items, _ = report_pdf._extract_list_items(markdown.splitlines(), 0)

    assert [item["marker"] for item in items] == ["1.", "•", "1."]


def test_build_list_flowables_keeps_nested_children():
    markdown = "- Parent\n  - Child\n    - Grandchild\n- Sibling\n"
    items, _ = report_pdf._extract_list_items(markdown.splitlines(), 0)

    flowables = report_pdf._build_list_flowables(
        report_pdf._nest_list_items(items),
        report_pdf._styles(),
    )

    assert len(flowables) == 1
    root_list = flowables[0]
    assert isinstance(root_list, ListFlowable)
    assert len(root_list._flowables) == 2

    first_item = root_list._flowables[0]
    child_lists = [flowable for flowable in first_item._flowables if isinstance(flowable, ListFlowable)]
    assert len(child_lists) == 1

    child_list = child_lists[0]
    assert len(child_list._flowables) == 1

    grandchild_lists = [
        flowable for flowable in child_list._flowables[0]._flowables if isinstance(flowable, ListFlowable)
    ]
    assert len(grandchild_lists) == 1


def test_table_column_widths_favor_content_heavy_columns():
    rows = [
        ["Target", "Evidence", "Priority"],
        ["GENE1", "A substantially longer evidence summary that needs room", "High"],
    ]

    widths = report_pdf._table_column_widths(rows, 504)

    assert sum(widths) == pytest.approx(504)
    assert widths[1] > widths[0]
    assert widths[1] > widths[2]


def test_write_markdown_pdf_falls_back_to_legacy_with_nested_lists(tmp_path, monkeypatch):
    markdown = "- Parent\n  - Child\n    - Grandchild\n- Sibling\n"
    output_path = tmp_path / "nested-lists.pdf"

    monkeypatch.setattr(report_pdf, "_write_markdown_pdf_chrome", lambda *args, **kwargs: "chrome unavailable")

    error = report_pdf.write_markdown_pdf(markdown, output_path, title="Nested list test")

    assert error is None
    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_build_html_document_normalizes_nested_list_indentation_for_chrome_path():
    markdown = "- Parent\n  - Child\n    - Grandchild\n- Sibling\n"

    html = report_pdf._build_html_document(markdown, title="Nested HTML")

    assert "<li>Parent<ul>" in html
    assert "<li>Child<ul>" in html
    assert "<li>Grandchild</li>" in html


def test_build_html_document_neutralizes_active_content_and_external_images():
    markdown = """
<a id="ref-1"></a>
Reference target.

<script src="https://example.test/script.js">alert("unsafe")</script>
<iframe src="file:///etc/passwd"></iframe>
![Tracking pixel](https://example.test/pixel.png)
"""

    html = report_pdf._build_html_document(markdown, title="Safe HTML")

    assert '<a id="ref-1"></a>' in html
    assert "<script" not in html
    assert "<iframe" not in html
    assert "<img" not in html
    assert "https://example.test/pixel.png" not in html
    assert "Content-Security-Policy" in html
    assert "default-src 'none'" in html


def test_write_markdown_pdf_rejects_invalid_primary_output_and_uses_fallback(tmp_path, monkeypatch):
    output_path = tmp_path / "validated-fallback.pdf"

    def invalid_primary(markdown, candidate_path, *, title):
        candidate_path.write_bytes(b"not a PDF")
        return None

    monkeypatch.setattr(report_pdf, "_write_markdown_pdf_chrome", invalid_primary)

    error = report_pdf.write_markdown_pdf("# Valid fallback", output_path)

    assert error is None
    assert report_pdf._validate_pdf_file(output_path) is None


def test_chromium_timeout_skips_redundant_second_attempt(tmp_path, monkeypatch):
    output_path = tmp_path / "timed-out.pdf"
    calls = 0

    def time_out(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise report_pdf.subprocess.TimeoutExpired(
            cmd=args[0],
            timeout=report_pdf.CHROME_RENDER_TIMEOUT_SECONDS,
        )

    monkeypatch.setattr(report_pdf, "_find_chrome_binary", lambda: "/fake/chrome")
    monkeypatch.setattr(report_pdf.subprocess, "run", time_out)

    error = report_pdf._write_markdown_pdf_chrome("# Slow report", output_path, title="Slow")

    assert calls == 1
    assert error is not None
    assert "timed out after 5 seconds" in error
    assert not output_path.exists()


def test_chromium_timeout_keeps_a_complete_pdf(tmp_path, monkeypatch):
    output_path = tmp_path / "completed-before-timeout.pdf"

    def finish_pdf_then_time_out(command, **kwargs):
        output_argument = next(value for value in command if value.startswith("--print-to-pdf="))
        Path(output_argument.split("=", 1)[1]).write_bytes(
            b"%PDF-1.4\ncomplete renderer output\n%%EOF\n"
        )
        raise report_pdf.subprocess.TimeoutExpired(
            cmd=command,
            timeout=report_pdf.CHROME_RENDER_TIMEOUT_SECONDS,
        )

    monkeypatch.setattr(report_pdf, "_find_chrome_binary", lambda: "/fake/chrome")
    monkeypatch.setattr(report_pdf.subprocess, "run", finish_pdf_then_time_out)

    error = report_pdf._write_markdown_pdf_chrome("# Complete report", output_path, title="Complete")

    assert error is None
    assert report_pdf._validate_pdf_file(output_path) is None


def test_write_markdown_pdf_preserves_existing_pdf_when_all_renderers_fail(tmp_path, monkeypatch):
    output_path = tmp_path / "existing.pdf"
    original_pdf = b"%PDF-1.4\nexisting known-good file\n%%EOF\n"
    output_path.write_bytes(original_pdf)

    def invalid_renderer(markdown, candidate_path, *, title):
        candidate_path.write_bytes(b"%PDF-1.4\ntruncated")
        return None

    monkeypatch.setattr(report_pdf, "_write_markdown_pdf_chrome", invalid_renderer)
    monkeypatch.setattr(report_pdf, "_write_markdown_pdf_legacy", invalid_renderer)

    error = report_pdf.write_markdown_pdf("# Replacement", output_path)

    assert error is not None
    assert "invalid PDF" in error
    assert output_path.read_bytes() == original_pdf
    assert not list(tmp_path.glob(f".{output_path.name}.*.tmp.pdf"))


def test_legacy_renderer_wraps_wide_tables_and_long_tokens(tmp_path, monkeypatch):
    headers = [f"Column {index}" for index in range(1, 9)]
    long_token = "rs1234567890" * 35
    markdown = "\n".join(
        [
            "# Wide evidence table",
            "",
            f"| {' | '.join(headers)} |",
            f"| {' | '.join(['---'] * len(headers))} |",
            f"| {' | '.join([long_token] * len(headers))} |",
        ]
    )
    output_path = tmp_path / "wide-table.pdf"
    monkeypatch.setattr(report_pdf, "_write_markdown_pdf_chrome", lambda *args, **kwargs: "chrome unavailable")

    error = report_pdf.write_markdown_pdf(markdown, output_path)

    assert error is None
    assert report_pdf._validate_pdf_file(output_path) is None
