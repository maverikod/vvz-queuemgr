"""
Working MCP Proxy Adapter server with queuemgr integration.

This is a complete working example that demonstrates how to use queuemgr
with mcp_proxy_adapter framework.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import logging
from typing import Dict, Any

from mcp_proxy_adapter.commands.base import Command
from mcp_proxy_adapter.core.errors import MicroserviceError
from mcp_proxy_adapter.api.app import create_app

from queuemgr.async_simple_queue import AsyncSimpleQueue
from queuemgr.jobs.base import QueueJobBase


# Example job for MCP adapter
class MCPJob(QueueJobBase):
    """Example job for MCP adapter."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize MCP job definition.

        Args:
            job_id: Unique identifier assigned by queue.
            params: Job configuration including task name and duration seconds.
        """
        super().__init__(job_id, params)
        self.task_name = params.get("task_name", "default_task")
        self.duration = params.get("duration", 3)

    def execute(self) -> None:
        """Execute the MCP job."""
        import time

        print(f"MCPJob {self.job_id}: Starting task '{self.task_name}'")

        # Simulate work
        for i in range(self.duration):
            time.sleep(1)
            progress = (i + 1) / self.duration * 100
            print(f"MCPJob {self.job_id}: Progress {progress:.1f}%")

        result = {
            "job_id": self.job_id,
            "task_name": self.task_name,
            "duration": self.duration,
            "completed_at": time.time(),
            "status": "completed",
        }

        self.set_result(result)
        print(f"MCPJob {self.job_id}: Task completed successfully")


# Global queue system
queue_system: AsyncSimpleQueue = None


class QueueCommand(Command):
    """Queue management command for MCP adapter."""

    def __init__(self):
        """Configure metadata for the queue management command."""
        super().__init__()
        self.name = "queue"
        self.description = "Queue management command for job operations"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [
                        "add_job",
                        "start_job",
                        "stop_job",
                        "get_status",
                        "list_jobs",
                        "delete_job",
                        "health",
                    ],
                    "description": "Action to perform",
                },
                "job_id": {"type": "string", "description": "Job identifier"},
                "task_name": {"type": "string", "description": "Task name for the job"},
                "duration": {
                    "type": "integer",
                    "description": "Duration in seconds",
                    "default": 3,
                },
            },
            "required": ["action"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute queue command."""
        try:
            action = params.get("action")
            job_id = params.get("job_id", "default_job")

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            if action == "add_job":
                task_name = params.get("task_name", "default_task")
                duration = params.get("duration", 3)

                await queue_system.add_job(
                    MCPJob, job_id, {"task_name": task_name, "duration": duration}
                )

                return {
                    "action": "add_job",
                    "job_id": job_id,
                    "task_name": task_name,
                    "duration": duration,
                    "message": f"Job {job_id} added successfully",
                }

            elif action == "start_job":
                await queue_system.start_job(job_id)
                return {
                    "action": "start_job",
                    "job_id": job_id,
                    "message": f"Job {job_id} started successfully",
                }

            elif action == "stop_job":
                await queue_system.stop_job(job_id)
                return {
                    "action": "stop_job",
                    "job_id": job_id,
                    "message": f"Job {job_id} stopped successfully",
                }

            elif action == "get_status":
                status = await queue_system.get_job_status(job_id)
                return {"action": "get_status", "job_id": job_id, "status": status}

            elif action == "list_jobs":
                jobs = await queue_system.list_jobs()
                return {"action": "list_jobs", "jobs": jobs, "count": len(jobs)}

            elif action == "delete_job":
                force = params.get("force", False)
                await queue_system.delete_job(job_id, force)
                return {
                    "action": "delete_job",
                    "job_id": job_id,
                    "message": f"Job {job_id} deleted successfully",
                }

            elif action == "health":
                is_running = queue_system.is_running()
                return {
                    "action": "health",
                    "queue_running": is_running,
                    "status": "healthy" if is_running else "unhealthy",
                }

            else:
                raise MicroserviceError(f"Unknown action: {action}")

        except Exception as e:
            raise MicroserviceError(f"Command failed: {str(e)}")


async def init_queue_system():
    """Initialize the queue system."""
    global queue_system
    queue_system = AsyncSimpleQueue(registry_path="/tmp/working_mcp_registry.jsonl")
    await queue_system.start()
    print("✅ Working queue system initialized")


async def cleanup_queue_system():
    """Cleanup the queue system."""
    global queue_system
    if queue_system:
        await queue_system.stop()
        print("✅ Working queue system stopped")


def create_working_mcp_app():
    """Create working MCP application."""
    app = create_app()

    # Register command
    from mcp_proxy_adapter.commands.command_registry import registry

    registry.register(QueueCommand())

    # Setup startup and cleanup
    @app.on_event("startup")
    async def startup_event():
        await init_queue_system()

    @app.on_event("shutdown")
    async def shutdown_event():
        await cleanup_queue_system()

    return app


async def main():
    """Main function."""
    print("🚀 Starting Working MCP Proxy Adapter with queuemgr")

    app = create_working_mcp_app()

    import uvicorn

    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)

    print("✅ Working MCP server started at http://localhost:8000")
    print("📋 Available command: queue")
    print("📝 Example usage:")
    print("  # Add a job")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "queue", "params": {"action": "add_job", "job_id": "test1", "task_name": "Test Task", "duration": 5}}\''
    )
    print()
    print("  # Start the job")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "queue", "params": {"action": "start_job", "job_id": "test1"}}\''
    )
    print()
    print("  # Check job status")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "queue", "params": {"action": "get_status", "job_id": "test1"}}\''
    )
    print()
    print("  # List all jobs")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print('    -d \'{"command": "queue", "params": {"action": "list_jobs"}}\'')
    print()
    print("  # Check queue health")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print('    -d \'{"command": "queue", "params": {"action": "health"}}\'')
    print()

    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
