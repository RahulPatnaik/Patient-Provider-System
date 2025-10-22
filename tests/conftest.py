"""
Pytest configuration and fixtures.
"""

import pytest
import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))


@pytest.fixture
def sample_env_vars():
    """Sample environment variables for testing."""
    return {
        "GEMINI_API_KEY": "test-gemini-key",
        "GROQ_API_KEY": "test-groq-key",
        "TEST_VAR": "test-value",
    }


@pytest.fixture
def mock_agent_name():
    """Mock agent name for testing."""
    from src.agents.base import AgentName
    return AgentName.SUPERVISOR


@pytest.fixture(autouse=True)
def cleanup_loggers():
    """Clean up loggers after each test to avoid handler duplication."""
    import logging
    yield
    # Clear all handlers from agent loggers
    for name in list(logging.Logger.manager.loggerDict.keys()):
        if name.startswith("agents."):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.propagate = True
