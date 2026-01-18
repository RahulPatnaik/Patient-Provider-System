"""
Task Orchestrator - Manages parallel agent execution for complex validation path.

KPME-focused (EL Branch):
- Runs Data Validator, Web Scraper, Enrichment, and Compliance agents in parallel
- Aggregates results from all agents
- Handles timeouts and errors gracefully
"""

import asyncio
from typing import Dict, Any, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class TaskOrchestrator:
    """
    Orchestrates parallel execution of validation agents.

    Runs multiple agents concurrently and aggregates results.
    """

    def __init__(self, timeout: int = 30):
        """
        Initialize orchestrator.

        Args:
            timeout: Timeout for agent execution in seconds
        """
        self.timeout = timeout
        logger.info(f"TaskOrchestrator initialized with {timeout}s timeout")

    async def run_agent_with_timeout(
        self,
        agent_name: str,
        agent_func: Any,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Run agent function with timeout.

        Args:
            agent_name: Name of the agent
            agent_func: Agent function to execute
            *args: Positional arguments for agent function
            **kwargs: Keyword arguments for agent function

        Returns:
            Agent result or error dict
        """
        try:
            logger.info(f"Starting {agent_name}...")
            result = await asyncio.wait_for(
                agent_func(*args, **kwargs),
                timeout=self.timeout
            )
            logger.info(f"{agent_name} completed successfully")
            return {
                "success": True,
                "agent": agent_name,
                "result": result,
                "error": None
            }
        except asyncio.TimeoutError:
            logger.error(f"{agent_name} timed out after {self.timeout}s")
            return {
                "success": False,
                "agent": agent_name,
                "result": None,
                "error": f"Timeout after {self.timeout}s"
            }
        except Exception as e:
            logger.error(f"{agent_name} failed: {str(e)}")
            return {
                "success": False,
                "agent": agent_name,
                "result": None,
                "error": str(e)
            }

    async def run_parallel(
        self,
        tasks: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Run multiple agents in parallel.

        Args:
            tasks: List of task dictionaries with 'name', 'func', 'args', 'kwargs'

        Returns:
            Dictionary with aggregated results
        """
        logger.info(f"Starting parallel execution of {len(tasks)} agents...")
        start_time = datetime.utcnow()

        # Create async tasks
        async_tasks = []
        for task in tasks:
            agent_name = task['name']
            agent_func = task['func']
            args = task.get('args', [])
            kwargs = task.get('kwargs', {})

            async_tasks.append(
                self.run_agent_with_timeout(agent_name, agent_func, *args, **kwargs)
            )

        # Run all tasks in parallel
        results = await asyncio.gather(*async_tasks, return_exceptions=True)

        # Aggregate results
        aggregated = {
            "execution_time": (datetime.utcnow() - start_time).total_seconds(),
            "total_agents": len(tasks),
            "successful_agents": sum(1 for r in results if isinstance(r, dict) and r.get('success')),
            "failed_agents": sum(1 for r in results if not isinstance(r, dict) or not r.get('success')),
            "results": {}
        }

        # Organize results by agent name
        for result in results:
            if isinstance(result, dict):
                agent_name = result.get('agent', 'unknown')
                aggregated['results'][agent_name] = result
            else:
                # Handle exceptions
                aggregated['results']['exception'] = {
                    "success": False,
                    "error": str(result)
                }

        logger.info(
            f"Parallel execution completed in {aggregated['execution_time']:.2f}s: "
            f"{aggregated['successful_agents']}/{aggregated['total_agents']} agents succeeded"
        )

        return aggregated


# Singleton instance
_orchestrator_instance: TaskOrchestrator | None = None


def get_orchestrator(timeout: int = 30) -> TaskOrchestrator:
    """Get singleton orchestrator instance."""
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = TaskOrchestrator(timeout=timeout)
    return _orchestrator_instance
