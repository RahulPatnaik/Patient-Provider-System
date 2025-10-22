import os
import time
import logging
from enum import Enum
from pathlib import Path
from contextlib import contextmanager, asynccontextmanager
from typing import Optional
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv


load_dotenv()

class AgentName(str, Enum):
    """Registry of all agent names."""
    SUPERVISOR = "supervisor"
    FAST_VALIDATOR = "fast_validator"
    DATA_VALIDATOR = "data_validator"
    WEB_SCRAPER = "web_scraper"
    ENRICHMENT = "enrichment"
    COMPLIANCE = "compliance"


class AgentError(Exception):
    """Base exception for all agent errors."""
    pass


class AgentTimeoutError(AgentError):
    """Agent execution timeout."""
    pass


class AgentValidationError(AgentError):
    """Agent validation failure."""
    pass

def get_env_var(key: str, default: Optional[str] = None) -> str:
    """Get environment variable with clear error if missing."""
    value = os.getenv(key, default)
    if value is None:
        raise ValueError(f"{key} not found in .env file")
    return value


@contextmanager
def track_execution_time():
    """Track execution time (sync). Yields dict with 'execution_time_ms'."""
    start = time.perf_counter()
    metrics = {}
    try:
        yield metrics
    finally:
        metrics["execution_time_ms"] = int((time.perf_counter() - start) * 1000)


@asynccontextmanager
async def track_execution_time_async():
    """Track execution time (async). Yields dict with 'execution_time_ms'."""
    start = time.perf_counter()
    metrics = {}
    try:
        yield metrics
    finally:
        metrics["execution_time_ms"] = int((time.perf_counter() - start) * 1000)


def get_agent_logger(agent_name: str | AgentName) -> logging.Logger:
    """
    Get logger with file rotation.
    Logs to: src/logs/{agent_name}.log (10MB max, 5 backups)
    """
    name = agent_name.value if isinstance(agent_name, AgentName) else agent_name
    logger = logging.getLogger(f"agents.{name}")

    if logger.handlers:
        return logger

    # Setup log directory and file
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"{name}.log"

    # Formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Configure
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    return logger

class BaseAgent:
    """
    Base class for all agents.

    Auto-setup: agent_name, logger
    Methods: get_env(), track_time(), track_time_async()
    import os
    import time
    import logging
    from enum import Enum


    Usage:
        class SupervisorAgent(BaseAgent):
            def __init__(self):
                super().__init__(AgentName.SUPERVISOR)
                api_key = self.get_env("GEMINI_API_KEY")
                self.agent = Agent('gemini-2.5-flash', ...)

            async def run(self, data):
                async with self.track_time_async() as timer:
                    result = await self.agent.run(data)
                self.logger.info(f"Done in {timer['execution_time_ms']}ms")
                return result.data
    """

    def __init__(self, agent_name: str | AgentName):
        self.agent_name = agent_name.value if isinstance(agent_name, AgentName) else agent_name
        self.logger = get_agent_logger(agent_name)

    def get_env(self, key: str, default: Optional[str] = None) -> str:
        """Get environment variable."""
        return get_env_var(key, default)

    @contextmanager
    def track_time(self):
        """Track sync execution time."""
        with track_execution_time() as metrics:
            yield metrics

    @asynccontextmanager
    async def track_time_async(self):
        """Track async execution time."""
        async with track_execution_time_async() as metrics:
            yield metrics
