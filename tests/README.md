# Tests

Comprehensive test suite for the Patient-Provider System.

## Running Tests

### Run All Tests
```bash
pytest
```

### Run with Coverage
```bash
pytest --cov=src --cov-report=html
```

### Run Specific Test File
```bash
pytest tests/unit/test_agents/test_base.py
```

### Run Specific Test
```bash
pytest tests/unit/test_agents/test_base.py::TestBaseAgent::test_base_agent_init_with_enum
```

### Run Tests by Marker
```bash
# Run only async tests
pytest -m asyncio

# Run only integration tests
pytest -m integration

# Run only e2e tests
pytest -m e2e
```

### Run with Verbose Output
```bash
pytest -v
```

### Run with Output (print statements)
```bash
pytest -s
```

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures and configuration
├── unit/                    # Unit tests
│   ├── test_agents/        # Agent tests
│   │   └── test_base.py   # BaseAgent tests
│   ├── test_core/         # Core logic tests
│   ├── test_models/       # Model tests
│   └── test_services/     # Service tests
├── integration/            # Integration tests
│   ├── test_api/          # API route tests
│   └── test_workflows/    # Workflow tests
└── e2e/                   # End-to-end tests
    └── test_validation_flow.py
```

## Test Coverage

Current test coverage for `base.py`:
- ✅ AgentName enum
- ✅ Exception classes
- ✅ get_env_var() function
- ✅ track_execution_time() (sync)
- ✅ track_execution_time_async() (async)
- ✅ get_agent_logger() function
- ✅ BaseAgent class initialization
- ✅ BaseAgent convenience methods
- ✅ BaseAgent subclassing

## Writing New Tests

### Test Naming Convention
- Test files: `test_*.py`
- Test classes: `Test*`
- Test functions: `test_*`

### Example Test
```python
import pytest
from src.agents.base import BaseAgent, AgentName

class TestMyFeature:
    def test_something(self):
        """Test description."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        assert agent.agent_name == "supervisor"

    @pytest.mark.asyncio
    async def test_async_something(self):
        """Test async functionality."""
        agent = BaseAgent(AgentName.SUPERVISOR)
        async with agent.track_time_async() as timer:
            pass
        assert "execution_time_ms" in timer
```

## Dependencies

Required for testing:
- pytest
- pytest-asyncio
- pytest-cov
- pytest-mock

Install with:
```bash
pip install pytest pytest-asyncio pytest-cov pytest-mock
```
