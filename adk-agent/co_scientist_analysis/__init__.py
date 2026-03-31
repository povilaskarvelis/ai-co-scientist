"""
AI Co-Scientist Analysis Agent Module

This module exports an analysis-mode root_agent for ADK evaluation.
"""
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.adk.apps.app import App

# Load environment variables
load_dotenv()

# Ensure the sibling `co_scientist` package is importable when ADK eval loads
# this package by file path.
PACKAGE_ROOT = Path(__file__).resolve().parent.parent
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from co_scientist.workflow import create_workflow_agent


# ADK evaluation looks for either `agent` or `root_agent`.
root_agent, _native_mcp_toolset = create_workflow_agent(
    require_plan_approval=True,
    workflow_mode="analysis",
)

app = App(
    name="co_scientist_analysis",
    root_agent=root_agent,
)

# Alias for ADK evaluation framework
agent = root_agent

__all__ = ["app", "agent", "root_agent", "create_workflow_agent"]
