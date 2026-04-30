#!/usr/bin/env python3
"""Install Cursor-native integration files for AI Co-Scientist.

This keeps `.agents/skills/biomedical-investigation/SKILL.md` as the canonical
agent-neutral skill, then mirrors it into `.cursor/skills/` for Cursor.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shutil
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CANONICAL_SKILL = ROOT / ".agents" / "skills" / "biomedical-investigation" / "SKILL.md"
CURSOR_SKILL = ROOT / ".cursor" / "skills" / "biomedical-investigation" / "SKILL.md"
CURSOR_MCP = ROOT / ".cursor" / "mcp.json"

MCP_CONFIG: dict[str, Any] = {
    "mcpServers": {
        "ai-co-scientist-research": {
            "type": "stdio",
            "command": "node",
            "args": ["${workspaceFolder}/research-mcp/server.js"],
            "envFile": "${workspaceFolder}/.env",
        }
    }
}


def formatted_json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def check_file(path: Path, expected: str, errors: list[str]) -> None:
    if not path.exists():
        errors.append(f"missing {path.relative_to(ROOT)}")
        return
    actual = path.read_text(encoding="utf-8")
    if actual != expected:
        errors.append(f"out of date {path.relative_to(ROOT)}")


def write_files() -> None:
    if not CANONICAL_SKILL.exists():
        raise FileNotFoundError(f"missing canonical skill: {CANONICAL_SKILL}")

    CURSOR_SKILL.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(CANONICAL_SKILL, CURSOR_SKILL)

    CURSOR_MCP.parent.mkdir(parents=True, exist_ok=True)
    CURSOR_MCP.write_text(formatted_json(MCP_CONFIG), encoding="utf-8")

    if not (ROOT / "research-mcp" / "node_modules").exists():
        print(
            "WARNING: research-mcp/node_modules is missing; run `cd research-mcp && npm install` "
            "before expecting Cursor to connect the project MCP server.",
            file=sys.stderr,
        )


def check_files() -> int:
    if not CANONICAL_SKILL.exists():
        print(f"ERROR: missing canonical skill: {CANONICAL_SKILL}", file=sys.stderr)
        return 1

    errors: list[str] = []
    check_file(CURSOR_SKILL, CANONICAL_SKILL.read_text(encoding="utf-8"), errors)
    check_file(CURSOR_MCP, formatted_json(MCP_CONFIG), errors)

    if errors:
        for error in errors:
            print(f"ERROR: Cursor integration {error}", file=sys.stderr)
        print("Run: python scripts/setup_cursor_integration.py", file=sys.stderr)
        return 1

    print("OK: Cursor integration files are up to date")
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync AI Co-Scientist skill and MCP config into .cursor/."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check files instead of writing them.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    if args.check:
        return check_files()

    write_files()
    print("OK: wrote .cursor skill and MCP config")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
