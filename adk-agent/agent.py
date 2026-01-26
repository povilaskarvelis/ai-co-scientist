"""
AI Co-Scientist Agent

This ADK agent connects to the research-mcp server to provide
drug target discovery capabilities using Gemini.

Usage:
    python agent.py         # Interactive mode
    python agent.py --help  # Show help

Requirements:
    - Node.js (for the MCP server)
    - Google API key in .env file
    - pip install -r requirements.txt

Setup:
    1. Edit .env file and paste your API key
    2. Run: python agent.py
"""
import os
import asyncio
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

from google.adk import Agent, Runner
from google.adk.tools import McpToolset
from mcp.client.stdio import StdioServerParameters
from google.adk.tools.mcp_tool.mcp_toolset import StdioConnectionParams

# Path to the MCP server
MCP_SERVER_DIR = Path(__file__).parent.parent / "research-mcp"


AGENT_INSTRUCTION = """You are an AI co-scientist specializing in preclinical drug target discovery.
Your goal is to help researchers explore biomedical data, generate hypotheses, and evaluate drug targets.

## Available Tools
You have access to 13 tools through the MCP server:

**Disease & Target Discovery:**
- search_diseases: Find diseases by name, get their IDs
- search_disease_targets: Find drug targets associated with a disease
- get_target_info: Get detailed information about a target
- search_targets: Search for targets by gene symbol

**Druggability Assessment:**
- check_druggability: Assess if a target can be targeted by drugs
- get_target_drugs: Find existing drugs for a target

**Clinical Evidence:**
- search_clinical_trials: Search ClinicalTrials.gov
- get_clinical_trial: Get detailed trial info including results

**Literature:**
- search_pubmed: Search PubMed for papers
- get_pubmed_abstract: Get full abstract for a paper

**Gene Information:**
- get_gene_info: Get gene details from NCBI

**Local Data:**
- list_local_datasets: List available local data files
- read_local_dataset: Read a local CSV/TSV file

## Workflow for Drug Target Discovery

1. **Understand the Disease**
   - Use search_diseases to find the disease and get its ID
   - Note related diseases that might share targets

2. **Identify Candidate Targets**
   - Use search_disease_targets to get targets ranked by evidence
   - Note the association scores and evidence types

3. **Evaluate Top Candidates**
   For each promising target:
   - Use get_target_info for biological function and pathways
   - Use check_druggability to assess if it can be targeted by drugs
   - Use get_target_drugs to see existing drug landscape

4. **Check Clinical Evidence**
   - Use search_clinical_trials to find relevant trials
   - Use get_clinical_trial for trials that have results
   - Pay special attention to TERMINATED trials - understand WHY they failed

5. **Gather Literature Support**
   - Use search_pubmed for recent research
   - Use get_pubmed_abstract for key papers

6. **Synthesize Recommendation**
   Provide a clear recommendation with:
   - Top target(s) ranked by potential
   - Evidence strength (cite PMIDs, NCT IDs)
   - Druggability assessment
   - Risks (failed trials, competition, safety concerns)
   - Suggested next steps

## Important Guidelines
- Always cite your sources with PMIDs or NCT IDs
- Acknowledge uncertainty when evidence is limited
- Highlight both opportunities AND risks
- Consider failed clinical trials as valuable negative evidence
- If a target has failed in trials, explain why it might still be worth pursuing (or not)

## Response Style
- Be thorough but concise
- Use structured formatting (headers, bullet points)
- Quantify when possible (e.g., "87% association score", "6 drugs in development")
- End with actionable recommendations
"""


def create_agent():
    """Create the ADK agent with MCP tools."""
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
    
    # Connect to the MCP server
    mcp_tools = McpToolset(connection_params=connection_params)
    
    # Create the agent
    agent = Agent(
        name="co_scientist",
        model="gemini-2.5-flash",
        instruction=AGENT_INSTRUCTION,
        tools=[mcp_tools],
    )
    
    return agent, mcp_tools


async def run_interactive_async():
    """Run the agent in interactive mode (async version)."""
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part
    
    print("=" * 60)
    print("AI Co-Scientist")
    print("=" * 60)
    
    # Check for API key
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key or api_key == "your-api-key-here":
        print("\n❌ GOOGLE_API_KEY not configured")
        print("\n   To fix this:")
        print("   1. Open .env file in the adk-agent folder")
        print("   2. Replace 'your-api-key-here' with your actual API key")
        print("   3. Get a free key at: https://aistudio.google.com/apikey")
        return
    
    print("\n✓ API key configured")
    print("Initializing agent with MCP server...")
    
    # Verify MCP server exists
    if not (MCP_SERVER_DIR / "server.js").exists():
        print(f"\n❌ MCP server not found at {MCP_SERVER_DIR}")
        print("\n   Make sure research-mcp/server.js exists")
        return
    
    try:
        agent, mcp_tools = create_agent()
    except Exception as e:
        print(f"\n❌ Failed to create agent: {e}")
        print("\n   Make sure:")
        print("   1. Node.js is installed")
        print("   2. Run: cd ../research-mcp && npm install")
        return
    
    # Get tool count to verify connection
    try:
        tools = await mcp_tools.get_tools()
        print(f"✓ Connected to MCP server ({len(tools)} tools available)")
    except Exception as e:
        print(f"\n❌ MCP connection failed: {e}")
        await mcp_tools.close()
        return
    
    # Create runner with session
    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name="co_scientist",
        session_service=session_service,
    )
    
    # Create a session
    session = await session_service.create_session(
        app_name="co_scientist",
        user_id="researcher",
    )
    
    print("\n✓ Agent ready!")
    print("\nExample queries:")
    print("  - 'Find promising drug targets for Parkinson's disease'")
    print("  - 'Evaluate LRRK2 as a drug target'")
    print("  - 'What clinical trials exist for Alzheimer's gamma-secretase inhibitors?'")
    print("\nType 'quit' to exit.\n")
    print("-" * 60)
    
    try:
        while True:
            try:
                # Use asyncio-compatible input
                user_input = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: input("\nYou: ").strip()
                )
                
                if user_input.lower() in ["quit", "exit", "q"]:
                    print("\nGoodbye!")
                    break
                
                if not user_input:
                    continue
                
                print("\nCo-Scientist: ", end="", flush=True)
                
                # Create Content object for the message
                message = Content(
                    role="user",
                    parts=[Part(text=user_input)]
                )
                
                # Run the agent and iterate over events (async)
                final_response = ""
                async for event in runner.run_async(
                    session_id=session.id,
                    user_id="researcher",
                    new_message=message,
                ):
                    # Check if this event has a response
                    if hasattr(event, 'content') and event.content:
                        if hasattr(event.content, 'parts'):
                            for part in event.content.parts:
                                if hasattr(part, 'text') and part.text:
                                    final_response += part.text
                                    print(part.text, end="", flush=True)
                
                if not final_response:
                    print("(No response generated)")
                else:
                    print()  # New line after response
                
            except KeyboardInterrupt:
                print("\n\nGoodbye!")
                break
            except Exception as e:
                print(f"\nError: {e}")
                import traceback
                traceback.print_exc()
                print("Please try again.")
    finally:
        # Cleanup MCP connection
        await mcp_tools.close()


def run_interactive():
    """Run the agent in interactive mode."""
    asyncio.run(run_interactive_async())


async def run_single_query_async(query: str):
    """Run a single query (async version)."""
    from google.adk.sessions import InMemorySessionService
    from google.genai.types import Content, Part
    
    agent, mcp_tools = create_agent()
    
    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name="co_scientist",
        session_service=session_service,
    )
    
    # Create session
    session = await session_service.create_session(
        app_name="co_scientist",
        user_id="researcher",
    )
    
    # Create message
    message = Content(
        role="user",
        parts=[Part(text=query)]
    )
    
    # Collect response from events
    response_text = ""
    async for event in runner.run_async(
        session_id=session.id,
        user_id="researcher",
        new_message=message,
    ):
        if hasattr(event, 'content') and event.content:
            if hasattr(event.content, 'parts'):
                for part in event.content.parts:
                    if hasattr(part, 'text') and part.text:
                        response_text += part.text
    
    # Cleanup
    await mcp_tools.close()
    
    return response_text if response_text else None


def run_single_query(query: str):
    """Run a single query (useful for testing)."""
    return asyncio.run(run_single_query_async(query))


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] in ["--help", "-h"]:
        print(__doc__)
        print("\nUsage: python agent.py")
    else:
        run_interactive()
