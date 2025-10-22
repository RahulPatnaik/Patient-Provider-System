"""
Tests for base agent utilities.
"""

import os
import pytest
import logging
import time
from pathlib import Path
from unittest.mock import patch, MagicMock

from src.agents.base import (
    AgentName,
    AgentError,
    AgentTimeoutError,
    AgentValidationError,
    get_env_var,
    track_execution_time,
    track_execution_time_async,
    get_agent_logger,
    BaseAgent,
)


# ============================================================================
# Test Enums
# ============================================================================


class TestAgentName:
    """Test AgentName enum."""

    def test_all_agent_names_exist(self):
        """Test that all expected agent names are defined."""
        assert AgentName.SUPERVISOR == "supervisor"
        assert AgentName.FAST_VALIDATOR == "fast_validator"
        assert AgentName.DATA_VALIDATOR == "data_validator"
        assert AgentName.WEB_SCRAPER == "web_scraper"
        assert AgentName.ENRICHMENT == "enrichment"
        assert AgentName.COMPLIANCE == "compliance"

    def test_agent_name_is_string_enum(self):
        """Test that AgentName values are strings."""
        assert isinstance(AgentName.SUPERVISOR.value, str)
        assert AgentName.SUPERVISOR.value == "supervisor"


# ============================================================================
# Test Exceptions
# ============================================================================


class TestExceptions:
    """Test custom exception classes."""

    def test_agent_error_is_exception(self):
        """Test that AgentError is an Exception."""
        assert issubclass(AgentError, Exception)

    def test_agent_timeout_error_inherits_from_agent_error(self):
        """Test that AgentTimeoutError inherits from AgentError."""
        assert issubclass(AgentTimeoutError, AgentError)

    def test_agent_validation_error_inherits_from_agent_error(self):
        """Test that AgentValidationError inherits from AgentError."""
        assert issubclass(AgentValidationError, AgentError)

    def test_can_raise_and_catch_agent_error(self):
        """Test raising and catching AgentError."""
        with pytest.raises(AgentError):
            raise AgentError("Test error")

    def test_can_catch_timeout_error_as_agent_error(self):
        """Test that AgentTimeoutError can be caught as AgentError."""
        with pytest.raises(AgentError):
            raise AgentTimeoutError("Timeout")


# ============================================================================
# Test get_env_var
# ============================================================================


class TestGetEnvVar:
    """Test get_env_var utility function."""

    def test_get_existing_env_var(self):
        """Test getting an existing environment variable."""
        with patch.dict(os.environ, {"TEST_KEY": "test_value"}):
            result = get_env_var("TEST_KEY")
            assert result == "test_value"

    def test_get_env_var_with_default(self):
        """Test getting env var with default when not found."""
        result = get_env_var("NONEXISTENT_KEY", default="default_value")
        assert result == "default_value"

    def test_get_env_var_missing_raises_error(self):
        """Test that missing env var without default raises ValueError."""
        with pytest.raises(ValueError, match="TEST_MISSING not found in .env file"):
            get_env_var("TEST_MISSING")

    def test_error_message_is_helpful(self):
        """Test that error message contains the key name."""
        try:
            get_env_var("MY_API_KEY")
        except ValueError as e:
            assert "MY_API_KEY" in str(e)
            assert ".env" in str(e)


# ============================================================================
# Test track_execution_time
# ============================================================================


class TestTrackExecutionTime:
    """Test execution time tracking (sync version)."""

    def test_tracks_execution_time(self):
        """Test that execution time is tracked."""
        with track_execution_time() as timer:
            time.sleep(0.01)  # Sleep for 10ms

        assert "execution_time_ms" in timer
        assert timer["execution_time_ms"] >= 10  # At least 10ms
        assert isinstance(timer["execution_time_ms"], int)

    def test_timer_dict_is_empty_before_exit(self):
        """Test that timer dict is populated on exit."""
        with track_execution_time() as timer:
            assert timer == {}  # Empty during execution

        assert "execution_time_ms" in timer  # Populated after

    def test_execution_time_increases_with_longer_operations(self):
        """Test that longer operations have higher execution time."""
        with track_execution_time() as timer1:
            time.sleep(0.01)

        with track_execution_time() as timer2:
            time.sleep(0.02)

        assert timer2["execution_time_ms"] > timer1["execution_time_ms"]


# ============================================================================
# Test track_execution_time_async
# ============================================================================


class TestTrackExecutionTimeAsync:
    """Test execution time tracking (async version)."""

    @pytest.mark.asyncio
    async def test_tracks_async_execution_time(self):
        """Test that async execution time is tracked."""
        import asyncio

        async with track_execution_time_async() as timer:
            await asyncio.sleep(0.01)  # Sleep for 10ms

        assert "execution_time_ms" in timer
        assert timer["execution_time_ms"] >= 10
        assert isinstance(timer["execution_time_ms"], int)

    @pytest.mark.asyncio
    async def test_async_timer_dict_is_empty_during_execution(self):
        """Test that async timer dict is populated on exit."""
        async with track_execution_time_async() as timer:
            assert timer == {}

        assert "execution_time_ms" in timer


# ============================================================================
# Test get_agent_logger
# ============================================================================


class TestGetAgentLogger:
    """Test get_agent_logger utility function."""

    def test_get_logger_with_string_name(self):
        """Test getting logger with string name."""
        logger = get_agent_logger("test_agent")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "agents.test_agent"

    def test_get_logger_with_enum_name(self):
        """Test getting logger with AgentName enum."""
        logger = get_agent_logger(AgentName.SUPERVISOR)
        assert isinstance(logger, logging.Logger)
        assert logger.name == "agents.supervisor"

    def test_logger_has_handlers(self):
        """Test that logger has file and console handlers."""
        logger = get_agent_logger("test_with_handlers")
        assert len(logger.handlers) == 2  # File + console

    def test_logger_creates_log_directory(self):
        """Test that logger creates log directory if it doesn't exist."""
        # Trigger logger creation which should create the directory
        get_agent_logger("test_log_dir")

        # Check that log directory exists
        from pathlib import Path
        # Go from tests/unit/test_agents -> project root -> src/logs
        log_dir = Path(__file__).parent.parent.parent.parent / "src" / "logs"
        assert log_dir.exists()

    def test_same_logger_returned_on_multiple_calls(self):
        """Test that calling get_agent_logger twice returns same logger."""
        logger1 = get_agent_logger("same_logger_test")
        logger2 = get_agent_logger("same_logger_test")
        assert logger1 is logger2


# ============================================================================
# Test BaseAgent
# ============================================================================


class TestBaseAgent:
    """Test BaseAgent class."""

    def test_base_agent_init_with_enum(self):
        """Test BaseAgent initialization with AgentName enum."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        assert agent.agent_name == "supervisor"
        assert isinstance(agent.logger, logging.Logger)

    def test_base_agent_init_with_string(self):
        """Test BaseAgent initialization with string name."""
        agent = BaseAgent("custom_agent")
        assert agent.agent_name == "custom_agent"
        assert isinstance(agent.logger, logging.Logger)

    def test_base_agent_logger_is_configured(self):
        """Test that BaseAgent logger is properly configured."""
        agent = BaseAgent(AgentName.DATA_VALIDATOR)
        assert agent.logger.name == "agents.data_validator"
        assert len(agent.logger.handlers) > 0

    def test_base_agent_get_env_method(self):
        """Test BaseAgent.get_env() method."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        with patch.dict(os.environ, {"TEST_VAR": "test_value"}):
            result = agent.get_env("TEST_VAR")
            assert result == "test_value"

    def test_base_agent_get_env_with_default(self):
        """Test BaseAgent.get_env() with default value."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        result = agent.get_env("NONEXISTENT", default="default")
        assert result == "default"

    def test_base_agent_track_time_method(self):
        """Test BaseAgent.track_time() method."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        with agent.track_time() as timer:
            time.sleep(0.01)

        assert "execution_time_ms" in timer
        assert timer["execution_time_ms"] >= 10

    @pytest.mark.asyncio
    async def test_base_agent_track_time_async_method(self):
        """Test BaseAgent.track_time_async() method."""
        import asyncio
        agent = BaseAgent(AgentName.SUPERVISOR)

        async with agent.track_time_async() as timer:
            await asyncio.sleep(0.01)

        assert "execution_time_ms" in timer
        assert timer["execution_time_ms"] >= 10


# ============================================================================
# Test BaseAgent Subclassing
# ============================================================================


class TestBaseAgentSubclassing:
    """Test that BaseAgent can be properly subclassed."""

    def test_can_subclass_base_agent(self):
        """Test creating a subclass of BaseAgent."""
        class TestAgent(BaseAgent):
            def __init__(self):
                super().__init__(AgentName.SUPERVISOR)
                self.custom_attribute = "test"

        agent = TestAgent()
        assert agent.agent_name == "supervisor"
        assert agent.custom_attribute == "test"
        assert isinstance(agent.logger, logging.Logger)

    def test_subclass_can_use_get_env(self):
        """Test that subclass can use get_env method."""
        class TestAgent(BaseAgent):
            def __init__(self):
                super().__init__(AgentName.SUPERVISOR)

        agent = TestAgent()
        with patch.dict(os.environ, {"API_KEY": "secret"}):
            result = agent.get_env("API_KEY")
            assert result == "secret"

    def test_subclass_can_use_track_time(self):
        """Test that subclass can use track_time method."""
        class TestAgent(BaseAgent):
            def __init__(self):
                super().__init__(AgentName.SUPERVISOR)

            def do_work(self):
                with self.track_time() as timer:
                    time.sleep(0.01)
                return timer

        agent = TestAgent()
        result = agent.do_work()
        assert "execution_time_ms" in result

    @pytest.mark.asyncio
    async def test_subclass_can_use_track_time_async(self):
        """Test that subclass can use track_time_async method."""
        import asyncio

        class TestAgent(BaseAgent):
            def __init__(self):
                super().__init__(AgentName.SUPERVISOR)

            async def do_async_work(self):
                async with self.track_time_async() as timer:
                    await asyncio.sleep(0.01)
                return timer

        agent = TestAgent()
        result = await agent.do_async_work()
        assert "execution_time_ms" in result
