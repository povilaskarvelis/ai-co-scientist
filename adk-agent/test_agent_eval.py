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


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_co_investigator_workflow():
    """Test co-investigator workflow behavior."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "co_investigator_workflow.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_co_investigator_general_contract():
    """Test generalized co-investigator response contract across request types."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "co_investigator_general_contract.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_researcher_discovery():
    """Test researcher discovery and author extraction pathways."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "researcher_discovery.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_variant_pathway():
    """Test variant and pathway/network evidence retrieval."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "variant_pathway.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_stress_eval():
    """Broad stress evaluation across diverse co-scientist request types."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "stress_eval.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_toolbox_expansion():
    """Evaluate newly added toolbox coverage for trial landscape, chemistry, and ontology expansion."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "toolbox_expansion.test.json"),
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_sprint2_context_direction():
    """Evaluate Sprint 2 capabilities: expression/cell context and genetics direction-of-effect."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "sprint2" / "sprint2_context_direction.test.json"),
        num_runs=1,
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_sprint3_competition_safety():
    """Evaluate Sprint 3 capabilities: competitive landscape and safety liabilities."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "sprint3" / "sprint3_competition_safety.test.json"),
        num_runs=1,
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_sprint4_target_ranking():
    """Evaluate Sprint 4 capabilities: multi-axis comparison and target ranking."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "sprint4" / "sprint4_target_ranking.test.json"),
        num_runs=1,
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_sprint5_custom_weights():
    """Evaluate Sprint 5 capabilities: custom axis weighting for target ranking."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "sprint5" / "sprint5_custom_weights.test.json"),
        num_runs=1,
        print_detailed_results=True,
    )


@pytest.mark.skipif(not ADK_EVAL_AVAILABLE, reason="ADK evaluation module not installed")
@pytest.mark.skipif(not os.environ.get("GOOGLE_API_KEY"), reason="GOOGLE_API_KEY not set")
@pytest.mark.asyncio
async def test_sprint6_auto_mode_clarification():
    """Evaluate Sprint 6 capabilities: auto strategy selection and minimal clarification."""
    await AgentEvaluator.evaluate(
        agent_module=AGENT_MODULE,
        eval_dataset_file_path_or_dir=str(EVALS_DIR / "sprint6" / "sprint6_auto_mode_clarification.test.json"),
        num_runs=1,
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
