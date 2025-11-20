"""
Simple MCP Proxy Adapter example with queuemgr.

This is a minimal example showing how to integrate queuemgr with mcp_proxy_adapter.

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


# Simple test job
class TestJob(QueueJobBase):
    """Simple test job for MCP adapter."""

    def __init__(self, job_id: str, params: dict):
        """
        Initialize job metadata used for demonstration.

        Args:
            job_id: Unique identifier for the job.
            params: Input parameters, expects optional message text.
        """
        super().__init__(job_id, params)
        self.message = params.get("message", "Default message")

    def execute(self) -> None:
        """Execute the test job."""
        import time

        print(f"TestJob {self.job_id}: Processing message: {self.message}")
        time.sleep(2)  # Simulate work

        result = {
            "job_id": self.job_id,
            "message": self.message,
            "processed_at": time.time(),
            "status": "completed",
        }

        self.set_result(result)
        print(f"TestJob {self.job_id}: Completed successfully")


# Global queue system
queue_system: AsyncSimpleQueue = None


class SimpleQueueCommand(Command):
    """Simple queue command for testing."""

    def __init__(self):
        """Configure metadata for the simple_queue MCP command."""
        super().__init__()
        self.name = "simple_queue"
        self.description = "Simple queue command for testing"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["add_job", "start_job", "get_status", "list_jobs"],
                    "description": "Action to perform",
                },
                "job_id": {"type": "string", "description": "Job identifier"},
                "message": {"type": "string", "description": "Message for the job"},
            },
            "required": ["action"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute simple queue command."""
        try:
            action = params.get("action")
            job_id = params.get("job_id", "default_job")
            message = params.get("message", "Hello from MCP!")

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            if action == "add_job":
                await queue_system.add_job(TestJob, job_id, {"message": message})
                return {
                    "action": "add_job",
                    "job_id": job_id,
                    "message": f"Job {job_id} added successfully",
                }

            elif action == "start_job":
                await queue_system.start_job(job_id)
                return {
                    "action": "start_job",
                    "job_id": job_id,
                    "message": f"Job {job_id} started successfully",
                }

            elif action == "get_status":
                status = await queue_system.get_job_status(job_id)
                return {"action": "get_status", "job_id": job_id, "status": status}

            elif action == "list_jobs":
                jobs = await queue_system.list_jobs()
                return {"action": "list_jobs", "jobs": jobs, "count": len(jobs)}

            else:
                raise MicroserviceError(f"Unknown action: {action}")

        except Exception as e:
            raise MicroserviceError(f"Command failed: {str(e)}")


async def init_queue_system():
    """Initialize the queue system."""
    global queue_system
    queue_system = AsyncSimpleQueue(registry_path="/tmp/simple_mcp_registry.jsonl")
    await queue_system.start()
    print("✅ Simple queue system initialized")


async def cleanup_queue_system():
    """Cleanup the queue system."""
    global queue_system
    if queue_system:
        await queue_system.stop()
        print("✅ Simple queue system stopped")


def create_simple_mcp_app():
    """Create simple MCP application."""
    app = create_app()

    # Register command
    from mcp_proxy_adapter.commands.command_registry import registry

    registry.register(SimpleQueueCommand())

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
    print("🚀 Starting Simple MCP Proxy Adapter with queuemgr")

    app = create_simple_mcp_app()

    import uvicorn

    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)

    print("✅ Simple MCP server started at http://localhost:8000")
    print("📋 Available command: simple_queue")
    print("📝 Example usage:")
    print("  # Add a job")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "simple_queue", "params": {"action": "add_job", "job_id": "test1", "message": "Hello World!"}}\''
    )
    print()
    print("  # Start the job")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "simple_queue", "params": {"action": "start_job", "job_id": "test1"}}\''
    )
    print()
    print("  # Check job status")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "simple_queue", "params": {"action": "get_status", "job_id": "test1"}}\''
    )
    print()

    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
