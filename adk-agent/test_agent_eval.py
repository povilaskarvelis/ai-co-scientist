#!/usr/bin/env python3
"""
Agent Evaluation Tests using ADK's built-in evaluation framework.

This tests the agent's behavior:
- Tool trajectory: Did the agent call the right tools?
- Response quality: Is the response relevant and accurate?
- Hallucinations: Did the agent make things up?

Run:
    pytest test_agent_eval.py -v
    
Or run specific tests:
    pytest test_agent_eval.py::test_parkinsons_discovery -v
"""
import pytest
import os
from pathlib import Path

# Ensure API key is loaded
from dotenv import load_dotenv
load_dotenv()


# Check if we can import ADK evaluation module
try:
    from google.adk.evaluation.agent_evaluator import AgentEvaluator
    ADK_EVAL_AVAILABLE = True
except ImportError:
    ADK_EVAL_AVAILABLE = False
    print("Warning: ADK evaluation module not available. Install with: pip install google-adk[eval]")


EVALS_DIR = Path(__file__).parent / "evals"


# Agent module path (relative to this file's directory)
AGENT_MODULE = "co_scientist"


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_parkinsons_discovery():
    """Test agent's ability to discover Parkinson's disease targets."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "parkinsons_discovery.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_target_evaluation():
    """Test agent's ability to evaluate target druggability."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "target_evaluation.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_clinical_trials():
    """Test agent's ability to find clinical trials."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "clinical_trials.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_literature_search():
    """Test agent's ability to search scientific literature."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "literature_search.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_all_evals():
    """Run all evaluation test files in the evals directory."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR),
        print_detailed_results=True,
    )


if __name__ == "__main__":
    # Quick check for requirements
    if not ADK_EVAL_AVAILABLE:
        print("❌ ADK evaluation module not available")
        print("   Install with: pip install google-adk[eval]")
        exit(1)
    
    if not os.environ.get("GOOGLE_API_KEY"):
        print("❌ GOOGLE_API_KEY not set")
        print("   Set your API key in .env file")
        exit(1)
    
    print("✓ Requirements met. Run tests with:")
    print("  pytest test_agent_eval.py -v")
