"""
Utilities for exporting markdown reports to PDF.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from xml.sax.saxutils import escape


try:  # pragma: no cover - exercised indirectly in environments with reportlab.
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import LETTER
    from reportlab.lib.styles import ListStyle, ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import (
        ListFlowable,
        ListItem,
        Paragraph,
        SimpleDocTemplate,
        Spacer,
        Table,
        TableStyle,
    )

    REPORTLAB_AVAILABLE = True
except Exception:  # pragma: no cover - fallback for minimal environments.
    REPORTLAB_AVAILABLE = False

try:  # pragma: no cover - exercised indirectly when markdown backend is available.
    import markdown as markdown_lib

    MARKDOWN_AVAILABLE = True
except Exception:  # pragma: no cover - fallback for minimal environments.
    MARKDOWN_AVAILABLE = False


CHROME_CANDIDATES = (
    "google-chrome",
    "chromium",
    "chromium-browser",
    "chrome",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
)
CHROME_RENDER_TIMEOUT_SECONDS = 5

HIGH_FIDELITY_CSS = """
@page {
  size: Letter;
  margin: 0.75in 0.75in 0.85in 0.75in;
}
html, body {
  margin: 0;
  padding: 0;
  background: #ffffff;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}
body {
  font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
  font-size: 10.5pt;
  line-height: 1.6;
  color: #1a1a1a;
  background: #ffffff;
}
article.report {
  max-width: 100%;
}

p, li, blockquote, th, td, h1, h2, h3 {
  overflow-wrap: anywhere;
  word-break: break-word;
}

/* ── Title / H1 ── */
h1 {
  font-size: 26pt;
  font-weight: 700;
  color: #111111;
  border-bottom: 3px solid #333333;
  padding-bottom: 0.35em;
  margin-top: 0;
  margin-bottom: 0.6em;
  line-height: 1.2;
  letter-spacing: -0.01em;
  break-after: avoid-page;
}

/* ── Section headings ── */
h2 {
  font-size: 14.5pt;
  font-weight: 700;
  color: #222222;
  margin-top: 1.5em;
  margin-bottom: 0.45em;
  padding-bottom: 0.2em;
  border-bottom: 1.5px solid #cccccc;
  line-height: 1.25;
  break-after: avoid-page;
}
h3 {
  font-size: 11.5pt;
  font-weight: 600;
  color: #333333;
  margin-top: 1.1em;
  margin-bottom: 0.3em;
  break-after: avoid-page;
}

/* ── Research question callout block ── */
blockquote {
  margin: 1em 0;
  padding: 0.8em 1.1em;
  border-left: 4px solid #555555;
  background: #f5f5f5;
  color: #333333;
  border-radius: 0 8px 8px 0;
  font-size: 10.5pt;
}
blockquote strong {
  color: #111111;
}

/* ── Body text ── */
p {
  margin: 0.5em 0;
  color: #1a1a1a;
}

/* ── Lists ── */
ul {
  list-style-type: disc;
  margin: 0.5em 0 0.5em 1.4em;
  padding-left: 0.8em;
  color: #1a1a1a;
}
ol {
  list-style-type: decimal;
  margin: 0.5em 0 0.5em 1.4em;
  padding-left: 0.8em;
  color: #1a1a1a;
}
li {
  margin: 0.35em 0;
  line-height: 1.55;
}
li::marker {
  color: #333333;
}

/* ── Code ── */
pre {
  margin: 0.8em 0;
  padding: 0.8em 1em;
  background: #f5f5f5;
  color: #1a1a1a;
  border: 1px solid #dddddd;
  border-radius: 8px;
  font-size: 9pt;
  overflow: hidden;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
code {
  font-family: "SFMono-Regular", Menlo, Consolas, monospace;
  font-size: 0.9em;
  background: #f0f0f0;
  padding: 0.15em 0.4em;
  border-radius: 4px;
  color: #1a1a1a;
}
pre code {
  background: transparent;
  padding: 0;
  color: inherit;
}

/* ── Tables ── */
table {
  width: 100%;
  table-layout: fixed;
  border-collapse: collapse;
  margin: 1em 0;
  font-size: 9.5pt;
}
thead {
  display: table-header-group;
}
th {
  background: #f0f0f0;
  color: #111111;
  font-weight: 600;
  padding: 0.5em 0.7em;
  text-align: left;
  border: 1px solid #cccccc;
}
td {
  border: 1px solid #cccccc;
  padding: 0.45em 0.65em;
  vertical-align: top;
  color: #1a1a1a;
}
tr:nth-child(even) td {
  background: #fafafa;
}
tr:nth-child(odd) td {
  background: #ffffff;
}

/* ── Horizontal rule ── */
hr {
  border: none;
  border-top: 1px solid #cccccc;
  margin: 1.3em 0;
}

/* ── Coverage / footer note ── */
em {
  color: #555555;
  font-size: 9.5pt;
}

/* ── Links ── */
a {
  color: #1a0dab;
  text-decoration: underline;
  word-break: break-all;
}
"""


_RAW_HTML_TAG_RE = re.compile(
    r"<!--.*?-->|<![A-Za-z][^>]*>|</?[A-Za-z][^>]*>",
    flags=re.DOTALL,
)
_ESCAPED_REFERENCE_ANCHOR_RE = re.compile(
    r'&lt;a\s+id=(["\'])ref-(\d+)\1\s*&gt;\s*&lt;/a&gt;',
    flags=re.IGNORECASE,
)
_RENDERED_IMAGE_RE = re.compile(r"<img\b[^>]*>", flags=re.IGNORECASE)


def _normalize_list_markers(text: str) -> str:
    """Replace ``* `` list markers with ``- `` to avoid ambiguity with emphasis."""
    return re.sub(r"^(\s*)\*(\s+)", r"\1-\2", text, flags=re.MULTILINE)


def _normalize_list_indentation(text: str) -> str:
    """
    Normalize nested list indentation so the HTML export path behaves more like
    the in-app preview, which treats any increased indent as a deeper level.
    """
    if not text:
        return ""

    lines = text.splitlines()
    normalized: list[str] = []
    indent_stack: list[int] = []
    current_item_indent: int | None = None
    current_item_level = 0
    in_code_block = False

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("```"):
            normalized.append(line)
            in_code_block = not in_code_block
            if not in_code_block:
                indent_stack.clear()
                current_item_indent = None
                current_item_level = 0
            continue

        if in_code_block:
            normalized.append(line)
            continue

        match = _match_list_item(line)
        if match:
            indent_width = _indent_width(match.group("indent") or "")
            level = _resolve_list_level(indent_width, indent_stack)
            marker = f"{match.group('num')}." if match.group("num") is not None else (match.group("bullet") or "-")
            normalized.append(f"{' ' * (level * 4)}{marker} {match.group('text').rstrip()}")
            current_item_indent = indent_width
            current_item_level = level
            continue

        if not stripped:
            normalized.append("")
            continue

        if current_item_indent is not None and indent_stack:
            leading = re.match(r"^(\s*)", line).group(1)
            indent_width = _indent_width(leading)
            if indent_width > current_item_indent:
                normalized_indent = (current_item_level * 4) + max(4, indent_width - current_item_indent)
                normalized.append(f"{' ' * normalized_indent}{line.lstrip()}")
                continue

        indent_stack.clear()
        current_item_indent = None
        current_item_level = 0
        normalized.append(line)

    result = "\n".join(normalized)
    if text.endswith("\n"):
        result += "\n"
    return result


def _prepare_markdown_for_pdf(text: str) -> str:
    return _normalize_list_indentation(_normalize_list_markers(text))


def _heading_level(line: str) -> int:
    if line.startswith("### "):
        return 3
    if line.startswith("## "):
        return 2
    if line.startswith("# "):
        return 1
    return 0


def _escape_attr(text: str) -> str:
    return escape(text, {'"': "&quot;"})


def _sanitize_markdown_html(text: str) -> str:
    """
    Neutralize raw HTML while preserving generated reference anchors.

    Reports are model-produced Markdown, so allowing arbitrary HTML would let
    scripts or resource-loading tags run inside the headless browser.
    """
    escaped_html = _RAW_HTML_TAG_RE.sub(lambda match: escape(match.group(0)), text or "")
    return _ESCAPED_REFERENCE_ANCHOR_RE.sub(
        lambda match: f'<a id="ref-{match.group(2)}"></a>',
        escaped_html,
    )


def _format_inline_markdown(text: str) -> str:
    """
    Convert common inline markdown to ReportLab Paragraph markup.
    """
    if not text:
        return ""

    placeholders: list[str] = []

    def _store(value: str) -> str:
        token = f"@@MD{len(placeholders)}@@"
        placeholders.append(value)
        return token

    def _replace_links(raw: str) -> str:
        pattern = re.compile(r"\[([^\]]+)\]\((https?://[^)]+)\)")

        def repl(match: re.Match[str]) -> str:
            label = escape(match.group(1).strip())
            href = _escape_attr(match.group(2).strip())
            return _store(f'<font color="#1a0dab"><u><link href="{href}">{label}</link></u></font>')

        return pattern.sub(repl, raw)

    def _replace_code(raw: str) -> str:
        pattern = re.compile(r"`([^`]+)`")

        def repl(match: re.Match[str]) -> str:
            value = escape(match.group(1).strip())
            return _store(f'<font name="Courier">{value}</font>')

        return pattern.sub(repl, raw)

    def _replace_bold(raw: str) -> str:
        pattern = re.compile(r"(\*\*|__)(.+?)\1")

        def repl(match: re.Match[str]) -> str:
            value = escape(match.group(2).strip())
            return _store(f"<b>{value}</b>")

        return pattern.sub(repl, raw)

    def _replace_italic(raw: str) -> str:
        # Require non-whitespace bounds inside emphasis to avoid treating list markers as italics.
        pattern = re.compile(
            r"(?<!\*)\*(?!\*)(\S(?:[^*]*?\S)?)\*(?!\*)|(?<!_)_(?!_)(\S(?:[^_]*?\S)?)_(?!_)"
        )

        def repl(match: re.Match[str]) -> str:
            value = match.group(1) if match.group(1) is not None else match.group(2)
            return _store(f"<i>{escape(value.strip())}</i>")

        return pattern.sub(repl, raw)

    transformed = text
    transformed = _replace_links(transformed)
    transformed = _replace_code(transformed)
    transformed = _replace_bold(transformed)
    transformed = _replace_italic(transformed)
    transformed = escape(transformed)

    for idx, value in enumerate(placeholders):
        transformed = transformed.replace(f"@@MD{idx}@@", value)

    return transformed


def _is_bold_only_line(line: str) -> bool:
    stripped = line.strip()
    return bool(re.fullmatch(r"(\*\*|__)(.+?)\1:?", stripped))


def _extract_bold_line_text(line: str) -> str:
    stripped = line.strip()
    match = re.fullmatch(r"(\*\*|__)(.+?)\1(:?)", stripped)
    if not match:
        return stripped
    suffix = ":" if match.group(3) else ""
    return f"{match.group(2).strip()}{suffix}"


def _strip_blockquote_prefix(line: str) -> str:
    return re.sub(r"^>+\s*", "", line.strip())


def _is_markdown_table_divider(line: str) -> bool:
    stripped = line.strip()
    if "|" not in stripped:
        return False
    cells = [cell.strip() for cell in stripped.strip("|").split("|")]
    if not cells:
        return False
    return all(bool(re.fullmatch(r":?-{1,}:?", cell or "")) for cell in cells)


def _is_markdown_table_start(lines: list[str], start_idx: int) -> bool:
    if start_idx + 1 >= len(lines):
        return False
    current = lines[start_idx].strip()
    next_line = lines[start_idx + 1].strip()
    return "|" in current and not _is_markdown_table_divider(current) and _is_markdown_table_divider(next_line)


def _split_markdown_table_row(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _find_chrome_binary() -> str | None:
    for candidate in CHROME_CANDIDATES:
        if os.path.isabs(candidate):
            if os.path.exists(candidate) and os.access(candidate, os.X_OK):
                return candidate
            continue
        resolved = shutil.which(candidate)
        if resolved:
            return resolved
    return None


def _build_html_document(markdown_text: str, *, title: str) -> str:
    safe_title = escape(str(title or "AI Co-Scientist Report"))
    normalized_markdown = _sanitize_markdown_html(_prepare_markdown_for_pdf(markdown_text))
    if MARKDOWN_AVAILABLE:
        body_html = markdown_lib.markdown(
            normalized_markdown,
            extensions=[
                "extra",
                "sane_lists",
                "fenced_code",
                "tables",
                "nl2br",
            ],
        )
    else:
        body_html = f"<pre>{escape(normalized_markdown)}</pre>"
    body_html = _RENDERED_IMAGE_RE.sub('<span class="omitted-image">[Image omitted]</span>', body_html)
    return (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\" />\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "  <meta name=\"color-scheme\" content=\"light\" />\n"
        "  <meta http-equiv=\"Content-Security-Policy\" "
        "content=\"default-src 'none'; style-src 'unsafe-inline'; img-src 'none'; "
        "font-src 'none'; script-src 'none'; connect-src 'none'; object-src 'none'; "
        "frame-src 'none'; base-uri 'none'; form-action 'none'\" />\n"
        f"  <title>{safe_title}</title>\n"
        f"  <style>{HIGH_FIDELITY_CSS}</style>\n"
        "</head>\n"
        "<body>\n"
        f"  <article class=\"report\">{body_html}</article>\n"
        "</body>\n"
        "</html>\n"
    )


def _write_markdown_pdf_chrome(markdown: str, output_path: Path, *, title: str) -> str | None:
    chrome_binary = _find_chrome_binary()
    if not chrome_binary:
        return "High-fidelity PDF export unavailable: Chrome/Chromium binary not found."
    if not MARKDOWN_AVAILABLE:
        return "High-fidelity PDF export unavailable: markdown package is not installed."

    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = _build_html_document(markdown, title=title)

    with tempfile.TemporaryDirectory(prefix="co_scientist_pdf_") as tmpdir:
        html_path = Path(tmpdir) / "report.html"
        html_path.write_text(html, encoding="utf-8")
        file_uri = html_path.resolve().as_uri()

        command_variants = [
            [
                chrome_binary,
                "--headless=new",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-background-networking",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-sync",
                "--metrics-recording-only",
                "--no-default-browser-check",
                "--no-first-run",
                f"--user-data-dir={str(Path(tmpdir) / 'chrome-profile-new')}",
                "--no-pdf-header-footer",
                "--print-to-pdf-no-header",
                f"--print-to-pdf={str(output_path)}",
                file_uri,
            ],
            [
                chrome_binary,
                "--headless",
                "--disable-gpu",
                "--no-sandbox",
                "--disable-background-networking",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-dev-shm-usage",
                "--disable-extensions",
                "--disable-sync",
                "--metrics-recording-only",
                "--no-default-browser-check",
                "--no-first-run",
                f"--user-data-dir={str(Path(tmpdir) / 'chrome-profile-legacy')}",
                "--no-pdf-header-footer",
                "--print-to-pdf-no-header",
                f"--print-to-pdf={str(output_path)}",
                file_uri,
            ],
        ]

        last_error = "Unknown Chromium rendering error."
        for command in command_variants:
            try:
                result = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=CHROME_RENDER_TIMEOUT_SECONDS,
                )
            except subprocess.TimeoutExpired:
                if _validate_pdf_file(output_path) is None:
                    return None
                last_error = (
                    "rendering timed out after "
                    f"{CHROME_RENDER_TIMEOUT_SECONDS} seconds"
                )
                break
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                continue

            if result.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
                return None

            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            detail = stderr or stdout or f"exit code {result.returncode}"
            last_error = detail[:300]

        if output_path.exists():
            output_path.unlink(missing_ok=True)
        return f"High-fidelity PDF export failed (Chromium): {last_error}"


def _indent_width(whitespace: str) -> int:
    return len((whitespace or "").expandtabs(4))


def _resolve_list_level(indent_width: int, indent_stack: list[int]) -> int:
    """
    Infer list nesting from observed indentation changes instead of assuming
    every nested level uses exactly four spaces.
    """
    if not indent_stack:
        indent_stack.append(indent_width)
        return 0

    while len(indent_stack) > 1 and indent_width < indent_stack[-1]:
        indent_stack.pop()

    if indent_width > indent_stack[-1]:
        indent_stack.append(indent_width)
    elif indent_width < indent_stack[-1]:
        indent_stack[-1] = indent_width

    return len(indent_stack) - 1


def _match_list_item(line: str):
    return re.match(
        r"^(?P<indent>\s*)(?:(?P<num>\d+)\.\s+|(?P<bullet>[-*+])\s+)(?P<text>.+)$",
        line,
    )


def _extract_list_items(lines: list[str], start_idx: int) -> tuple[list[dict], int]:
    items: list[dict] = []
    idx = start_idx
    ordered_counters: dict[int, int] = {}
    level_indents: list[int] = []
    list_kinds: dict[int, str] = {}

    while idx < len(lines):
        raw = lines[idx]
        if not raw.strip():
            look = idx + 1
            while look < len(lines) and not lines[look].strip():
                look += 1
            if look < len(lines) and _match_list_item(lines[look]):
                idx = look
                continue
            break

        match = _match_list_item(raw)
        if not match:
            break

        indent_width = _indent_width(match.group("indent") or "")
        level = _resolve_list_level(indent_width, level_indents)

        for depth in list(ordered_counters.keys()):
            if depth > level:
                del ordered_counters[depth]
        for depth in list(list_kinds.keys()):
            if depth > level:
                del list_kinds[depth]

        text_parts = [match.group("text").strip()]
        idx += 1

        while idx < len(lines):
            candidate = lines[idx]
            candidate_stripped = candidate.strip()
            if not candidate_stripped:
                look = idx + 1
                while look < len(lines) and not lines[look].strip():
                    look += 1
                if look >= len(lines):
                    idx = look
                    break

                next_match = _match_list_item(lines[look])
                if next_match:
                    next_indent = _indent_width(next_match.group("indent") or "")
                    if next_indent <= indent_width:
                        idx = look
                        break
                    idx = look
                    break

                next_indent = _indent_width(re.match(r"^(\s*)", lines[look]).group(1))
                if next_indent > indent_width:
                    text_parts.append(lines[look].strip())
                    idx = look + 1
                    continue
                idx = look
                break

            next_match = _match_list_item(candidate)
            if next_match:
                next_indent = _indent_width(next_match.group("indent") or "")
                if next_indent <= indent_width:
                    break
                break

            continuation_indent = _indent_width(re.match(r"^(\s*)", candidate).group(1))
            if continuation_indent > indent_width:
                text_parts.append(candidate_stripped)
                idx += 1
                continue
            break

        if match.group("num") is not None:
            source_number = int(match.group("num"))
            if list_kinds.get(level) != "ordered" or level not in ordered_counters:
                ordered_counters[level] = max(0, source_number - 1)
            ordered_counters[level] += 1
            marker = f"{ordered_counters[level]}."
            kind = "ordered"
        else:
            marker = "•"
            kind = "unordered"
            ordered_counters.pop(level, None)

        list_kinds[level] = kind

        items.append(
            {
                "kind": kind,
                "level": level,
                "marker": marker,
                "text": " ".join(text_parts).strip(),
            }
        )

    return items, idx


def _nest_list_items(items: list[dict]) -> list[dict]:
    roots: list[dict] = []
    stack: list[dict] = []

    for item in items:
        node = {**item, "children": []}
        level = int(node.get("level", 0))

        while stack and int(stack[-1].get("level", 0)) >= level:
            stack.pop()

        if stack:
            stack[-1]["children"].append(node)
        else:
            roots.append(node)

        stack.append(node)

    return roots


def _styles() -> dict[str, ParagraphStyle]:
    sample = getSampleStyleSheet()
    return {
        "body": ParagraphStyle(
            "report_body",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=14,
            spaceAfter=4,
            textColor=colors.HexColor("#1a1a1a"),
            wordWrap="CJK",
        ),
        "list_body": ParagraphStyle(
            "report_list_body",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=10.5,
            leading=14,
            spaceAfter=0,
            textColor=colors.HexColor("#1a1a1a"),
            wordWrap="CJK",
        ),
        "table_body": ParagraphStyle(
            "report_table_body",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=9,
            leading=11.5,
            spaceAfter=0,
            textColor=colors.HexColor("#1a1a1a"),
            wordWrap="CJK",
        ),
        "table_body_compact": ParagraphStyle(
            "report_table_body_compact",
            parent=sample["BodyText"],
            fontName="Helvetica",
            fontSize=8.25,
            leading=10.5,
            spaceAfter=0,
            textColor=colors.HexColor("#1a1a1a"),
            wordWrap="CJK",
        ),
        "table_header": ParagraphStyle(
            "report_table_header",
            parent=sample["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=9,
            leading=11.5,
            spaceAfter=0,
            textColor=colors.HexColor("#111111"),
            wordWrap="CJK",
        ),
        "table_header_compact": ParagraphStyle(
            "report_table_header_compact",
            parent=sample["BodyText"],
            fontName="Helvetica-Bold",
            fontSize=8.25,
            leading=10.5,
            spaceAfter=0,
            textColor=colors.HexColor("#111111"),
            wordWrap="CJK",
        ),
        "h1": ParagraphStyle(
            "report_h1",
            parent=sample["Heading1"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            spaceAfter=10,
            textColor=colors.HexColor("#111111"),
            wordWrap="CJK",
        ),
        "h2": ParagraphStyle(
            "report_h2",
            parent=sample["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=14,
            leading=18,
            spaceAfter=8,
            textColor=colors.HexColor("#222222"),
            wordWrap="CJK",
        ),
        "h3": ParagraphStyle(
            "report_h3",
            parent=sample["Heading3"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=16,
            spaceAfter=6,
            textColor=colors.HexColor("#333333"),
            wordWrap="CJK",
        ),
        "code": ParagraphStyle(
            "report_code",
            parent=sample["Code"],
            fontName="Courier",
            fontSize=9,
            leading=12,
            leftIndent=10,
            rightIndent=10,
            spaceAfter=6,
            textColor=colors.HexColor("#1a1a1a"),
            backColor=colors.HexColor("#f5f5f5"),
            wordWrap="CJK",
        ),
        "quote": ParagraphStyle(
            "report_quote",
            parent=sample["BodyText"],
            fontName="Helvetica-Oblique",
            fontSize=10,
            leading=13,
            leftIndent=14,
            rightIndent=8,
            textColor=colors.HexColor("#333333"),
            backColor=colors.HexColor("#f5f5f5"),
            borderPadding=6,
            spaceAfter=6,
            wordWrap="CJK",
        ),
    }


def _flush_paragraph(buffer: list[str], story: list, styles: dict[str, ParagraphStyle]) -> None:
    if not buffer:
        return
    text = " ".join(item.strip() for item in buffer if item.strip())
    buffer.clear()
    if not text:
        return
    story.append(Paragraph(_format_inline_markdown(text), styles["body"]))
    story.append(Spacer(1, 4))


def _group_list_nodes(nodes: list[dict]) -> list[list[dict]]:
    groups: list[list[dict]] = []
    current: list[dict] = []
    current_kind = ""

    for node in nodes:
        kind = str(node.get("kind", "unordered"))
        if current and kind != current_kind:
            groups.append(current)
            current = []
        current.append(node)
        current_kind = kind

    if current:
        groups.append(current)

    return groups


def _list_style(kind: str, level: int) -> ListStyle:
    return ListStyle(
        f"report_list_{kind}_{level}",
        leftIndent=18 + (level * 14),
        rightIndent=0,
        bulletType="1" if kind == "ordered" else "bullet",
        bulletColor=colors.HexColor("#333333"),
        bulletFontName="Helvetica",
        bulletFontSize=10,
        bulletOffsetY=0,
        bulletDedent="auto",
    )


def _build_list_flowables(nodes: list[dict], styles: dict[str, ParagraphStyle]) -> list:
    flowables: list = []

    for group in _group_list_nodes(nodes):
        if not group:
            continue

        kind = str(group[0].get("kind", "unordered"))
        level = int(group[0].get("level", 0))
        list_kwargs = {"style": _list_style(kind, level)}

        if kind == "ordered":
            marker = str(group[0].get("marker", "1.")).rstrip(".")
            try:
                list_kwargs["start"] = int(marker)
            except ValueError:
                pass

        items: list[ListItem] = []
        for node in group:
            text_markup = _format_inline_markdown(str(node.get("text", ""))).strip() or "&nbsp;"
            item_flowables: list = [Paragraph(text_markup, styles["list_body"])]
            item_flowables.extend(_build_list_flowables(node.get("children", []), styles))
            items.append(ListItem(item_flowables))

        flowables.append(ListFlowable(items, **list_kwargs))

    return flowables


def _add_list_block(lines: list[str], start_idx: int, story: list, styles: dict[str, ParagraphStyle]) -> int:
    items, idx = _extract_list_items(lines, start_idx)
    for flowable in _build_list_flowables(_nest_list_items(items), styles):
        story.append(flowable)
    if items:
        story.append(Spacer(1, 4))
    return idx


def _add_code_block(lines: list[str], start_idx: int, story: list, styles: dict[str, ParagraphStyle]) -> int:
    idx = start_idx + 1
    code_lines: list[str] = []
    while idx < len(lines):
        stripped = lines[idx].strip()
        if stripped.startswith("```"):
            idx += 1
            break
        code_lines.append(lines[idx].rstrip())
        idx += 1
    code_text = "<br/>".join(escape(item) if item else "&nbsp;" for item in code_lines) or "&nbsp;"
    story.append(Paragraph(code_text, styles["code"]))
    story.append(Spacer(1, 4))
    return idx


def _add_blockquote(lines: list[str], start_idx: int, story: list, styles: dict[str, ParagraphStyle]) -> int:
    idx = start_idx
    chunks: list[str] = []
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped.startswith(">"):
            break
        cleaned = _strip_blockquote_prefix(stripped)
        if cleaned:
            chunks.append(cleaned)
        idx += 1
    quote_text = " ".join(chunks).strip() or "&nbsp;"
    story.append(Paragraph(_format_inline_markdown(quote_text), styles["quote"]))
    story.append(Spacer(1, 4))
    return idx


def _table_column_widths(rows: list[list[str]], available_width: float) -> list[float]:
    max_cols = max((len(row) for row in rows), default=0)
    if max_cols <= 0:
        return []

    minimum_width = 38 if max_cols >= 7 else 48
    base_width = min(minimum_width, available_width / max_cols)
    remaining_width = max(0.0, available_width - (base_width * max_cols))
    weights: list[int] = []
    for column_index in range(max_cols):
        longest_cell = max(
            (
                len(re.sub(r"[*_\x60]", "", row[column_index]))
                for row in rows
                if column_index < len(row)
            ),
            default=1,
        )
        weights.append(max(1, min(longest_cell, 28)))

    total_weight = sum(weights)
    return [
        base_width + (remaining_width * weight / total_weight)
        for weight in weights
    ]


def _add_table(lines: list[str], start_idx: int, story: list, styles: dict[str, ParagraphStyle]) -> int:
    header = _split_markdown_table_row(lines[start_idx])
    idx = start_idx + 2  # Skip divider row.
    body_rows: list[list[str]] = []
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped or "|" not in stripped:
            break
        if _is_markdown_table_divider(stripped):
            idx += 1
            continue
        body_rows.append(_split_markdown_table_row(stripped))
        idx += 1

    rows = [header] + body_rows
    max_cols = max((len(row) for row in rows), default=0)
    normalized_rows = [row + [""] * (max_cols - len(row)) for row in rows]
    compact = max_cols >= 7
    body_style = styles["table_body_compact" if compact else "table_body"]
    header_style = styles["table_header_compact" if compact else "table_header"]
    table_data = []
    for row_index, row in enumerate(normalized_rows):
        cell_style = header_style if row_index == 0 else body_style
        table_data.append(
            [Paragraph(_format_inline_markdown(cell), cell_style) for cell in row]
        )

    available_width = LETTER[0] - 108
    column_widths = _table_column_widths(normalized_rows, available_width)
    table = Table(
        table_data,
        colWidths=column_widths,
        hAlign="LEFT",
        repeatRows=1,
        splitByRow=1,
        splitInRow=1,
    )
    table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#cccccc")),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#f0f0f0")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111111")),
                ("TEXTCOLOR", (0, 1), (-1, -1), colors.HexColor("#1a1a1a")),
                ("BACKGROUND", (0, 1), (-1, -1), colors.HexColor("#ffffff")),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#ffffff"), colors.HexColor("#fafafa")]),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 6))
    return idx


def _markdown_story(markdown: str) -> list:
    styles = _styles()
    story: list = []
    paragraph_buffer: list[str] = []
    lines = _prepare_markdown_for_pdf(markdown).splitlines()
    idx = 0
    while idx < len(lines):
        stripped = lines[idx].strip()
        if not stripped:
            _flush_paragraph(paragraph_buffer, story, styles)
            idx += 1
            continue

        if stripped.startswith("```"):
            _flush_paragraph(paragraph_buffer, story, styles)
            idx = _add_code_block(lines, idx, story, styles)
            continue

        if stripped.startswith(">"):
            _flush_paragraph(paragraph_buffer, story, styles)
            idx = _add_blockquote(lines, idx, story, styles)
            continue

        if _is_markdown_table_start(lines, idx):
            _flush_paragraph(paragraph_buffer, story, styles)
            idx = _add_table(lines, idx, story, styles)
            continue

        heading_level = _heading_level(stripped)
        if heading_level:
            _flush_paragraph(paragraph_buffer, story, styles)
            heading_text = stripped[heading_level + 1 :].strip()
            style_key = f"h{heading_level}"
            story.append(Paragraph(_format_inline_markdown(heading_text), styles[style_key]))
            story.append(Spacer(1, 4))
            idx += 1
            continue

        if _is_bold_only_line(stripped):
            _flush_paragraph(paragraph_buffer, story, styles)
            heading_text = _extract_bold_line_text(stripped)
            story.append(Paragraph(_format_inline_markdown(heading_text), styles["h3"]))
            story.append(Spacer(1, 4))
            idx += 1
            continue

        if _match_list_item(lines[idx]):
            _flush_paragraph(paragraph_buffer, story, styles)
            idx = _add_list_block(lines, idx, story, styles)
            continue

        paragraph_buffer.append(stripped)
        idx += 1

    _flush_paragraph(paragraph_buffer, story, styles)
    if not story:
        story.append(Paragraph("No content.", styles["body"]))
    return story


def _write_markdown_pdf_legacy(markdown: str, output_path: Path, *, title: str = "AI Co-Scientist Report") -> str | None:
    """
    Export markdown text to PDF using the legacy ReportLab renderer.

    Returns:
        None on success, otherwise an error message.
    """
    if not REPORTLAB_AVAILABLE:
        return "PDF export unavailable: reportlab is not installed."

    output_path.parent.mkdir(parents=True, exist_ok=True)
    # Strip HTML anchor tags inserted for Chrome internal links — they render
    # as literal text in the ReportLab path.
    clean_markdown = re.sub(r'<a\s[^>]*></a>', '', (markdown or "").strip())
    try:
        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=LETTER,
            title=title,
            leftMargin=54,
            rightMargin=54,
            topMargin=54,
            bottomMargin=54,
        )
        story = _markdown_story(clean_markdown)
        doc.build(story)
    except Exception as exc:
        if output_path.exists():
            output_path.unlink(missing_ok=True)
        return f"PDF export failed ({type(exc).__name__}): {exc}"

    return None


def _validate_pdf_file(path: Path) -> str | None:
    """Return an error when a renderer did not produce a complete PDF file."""
    try:
        if not path.is_file():
            return "renderer did not create an output file"
        file_size = path.stat().st_size
        if file_size < 14:
            return "renderer produced an empty or truncated file"
        with path.open("rb") as pdf_file:
            header = pdf_file.read(8)
            if not header.startswith(b"%PDF-"):
                return "output does not have a PDF header"
            pdf_file.seek(max(0, file_size - 4096))
            trailer = pdf_file.read()
        if not trailer.rstrip().endswith(b"%%EOF"):
            return "output does not have a complete PDF trailer"
    except OSError as exc:
        return f"could not inspect renderer output ({type(exc).__name__}: {exc})"
    return None


def _render_pdf_candidate(
    renderer,
    markdown: str,
    candidate_path: Path,
    *,
    title: str,
    backend_name: str,
) -> str | None:
    candidate_path.unlink(missing_ok=True)
    try:
        error = renderer(markdown, candidate_path, title=title)
    except Exception as exc:  # noqa: BLE001
        error = f"{backend_name} renderer raised {type(exc).__name__}: {exc}"
    if error:
        candidate_path.unlink(missing_ok=True)
        return error

    validation_error = _validate_pdf_file(candidate_path)
    if validation_error:
        candidate_path.unlink(missing_ok=True)
        return f"{backend_name} renderer produced an invalid PDF: {validation_error}."
    return None


def write_markdown_pdf(markdown: str, output_path: Path, *, title: str = "AI Co-Scientist Report") -> str | None:
    """
    Export markdown text to PDF.

    Backend order:
    1) High-fidelity HTML/CSS via headless Chrome/Chromium
    2) Legacy ReportLab fallback
    """
    normalized = str(markdown or "").strip()
    safe_title = str(title or "AI Co-Scientist Report")
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        dir=output_path.parent,
        prefix=f".{output_path.name}.",
        suffix=".tmp.pdf",
    )
    os.close(descriptor)
    candidate_path = Path(temporary_name)
    try:
        high_fidelity_error = _render_pdf_candidate(
            _write_markdown_pdf_chrome,
            normalized,
            candidate_path,
            title=safe_title,
            backend_name="Chromium",
        )
        if high_fidelity_error is None:
            try:
                os.replace(candidate_path, output_path)
            except OSError as exc:
                return f"PDF export failed while publishing the generated file ({type(exc).__name__}: {exc})"
            return None

        legacy_error = _render_pdf_candidate(
            _write_markdown_pdf_legacy,
            normalized,
            candidate_path,
            title=safe_title,
            backend_name="ReportLab",
        )
        if legacy_error is None:
            try:
                os.replace(candidate_path, output_path)
            except OSError as exc:
                return f"PDF export failed while publishing the generated file ({type(exc).__name__}: {exc})"
            return None

        return f"{high_fidelity_error} Fallback renderer error: {legacy_error}"
    finally:
        candidate_path.unlink(missing_ok=True)
