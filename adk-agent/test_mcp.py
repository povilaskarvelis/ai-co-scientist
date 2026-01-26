#!/usr/bin/env python3
"""Test MCP connection to research-mcp server."""
import os
import sys
import asyncio
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

MCP_SERVER_DIR = Path(__file__).parent.parent / "research-mcp"

from mcp.client.stdio import StdioServerParameters
from google.adk.tools.mcp_tool.mcp_toolset import StdioConnectionParams
from google.adk.tools import MCPToolset

async def test_mcp():
    print(f"MCP Server Dir: {MCP_SERVER_DIR}")
    print(f"Server.js exists: {(MCP_SERVER_DIR / 'server.js').exists()}")
    
    # Configure MCP server connection
    server_params = StdioServerParameters(
        command="node",
        args=["server.js"],
        cwd=str(MCP_SERVER_DIR),
    )
    
    connection_params = StdioConnectionParams(
        server_params=server_params,
        timeout=30.0,
    )
    
    print("\nCreating MCPToolset...")
    mcp_tools = MCPToolset(connection_params=connection_params)
    print(f"MCPToolset created: {mcp_tools}")
    
    # Get tools
    print("\nFetching tools from MCP server...")
    tools = await mcp_tools.get_tools()
    print(f"\n✓ Tools found: {len(tools)}")
    for tool in tools:
        print(f"  - {tool.name}")
    
    # Cleanup
    await mcp_tools.close()
    print("\n✓ MCP connection test successful!")

if __name__ == "__main__":
    asyncio.run(test_mcp())
