"""
AI Co-Scientist Agent Module

This module exports the root_agent for ADK evaluation framework.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from google.adk import Agent
from google.adk.tools import McpToolset
from mcp.client.stdio import StdioServerParameters
from google.adk.tools.mcp_tool.mcp_toolset import StdioConnectionParams

# Path to the MCP server
MCP_SERVER_DIR = Path(__file__).parent.parent.parent / "research-mcp"

AGENT_INSTRUCTION = """You are an AI co-scientist for biomedical investigation.
Operate as a general-purpose co-investigator across the full workflow:
scope the question, plan adaptively, execute evidence retrieval, critique uncertainty,
and revise strategy when evidence is weak, conflicting, or incomplete.

Follow these high-level principles:
- Maintain high workflow quality across scoping, planning, execution, reflection, and adaptation.
- Adapt both execution strategy and response structure to the query and evidence quality.
- Ground major claims in executed evidence and cite references where possible.
- Explicitly surface uncertainty, contradictory findings, and key limitations.
- Mention tools in narrative using real-world source names, not internal tool IDs.
- Integrate evidence, uncertainty, and references into a clear conclusion.
- Use only sections, bullets, or tables that improve clarity for the specific query.
"""


def _create_mcp_toolset():
    """Create MCP toolset for the agent."""
    server_params = StdioServerParameters(
        command="node",
        args=["server.js"],
        cwd=str(MCP_SERVER_DIR),
    )
    
    connection_params = StdioConnectionParams(
        server_params=server_params,
        timeout=90.0,
    )
    
    return McpToolset(connection_params=connection_params)


# Create the agent for ADK evaluation
# Note: MCP tools are lazily initialized when the agent runs
# ADK evaluation looks for either 'agent' or 'root_agent'
root_agent = Agent(
    name="co_scientist",
    model="gemini-2.5-flash",  # Using 2.5 flash
    instruction=AGENT_INSTRUCTION,
    tools=[_create_mcp_toolset()],
)

# Alias for ADK evaluation framework
agent = root_agent
