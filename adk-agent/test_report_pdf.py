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
