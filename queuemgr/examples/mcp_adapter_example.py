"""
MCP Proxy Adapter integration example with queuemgr.

This example demonstrates how to use queuemgr with mcp_proxy_adapter
framework for creating microservices with job queue capabilities.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import logging
from typing import Dict, Any, List
from pathlib import Path

from mcp_proxy_adapter.commands.base import Command
from mcp_proxy_adapter.commands.result import SuccessResult, ErrorResult
from mcp_proxy_adapter.core.errors import MicroserviceError, ValidationError
from mcp_proxy_adapter.api.app import create_app
from mcp_proxy_adapter.config import Config

from queuemgr.async_simple_api import AsyncQueueSystem, async_queue_system_context
from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.types import JobStatus


# Global queue system
queue_system: AsyncQueueSystem = None


class DataProcessingJob(QueueJobBase):
    """Example data processing job for MCP adapter."""

    def __init__(self, job_id: str, data: Dict[str, Any], operation: str = "process"):
        """
        Initialize DataProcessingJob.

        Args:
            job_id: Unique job identifier.
            data: Data to process.
            operation: Type of operation to perform.
        """
        super().__init__(job_id)
        self.data = data
        self.operation = operation

    def run(self) -> None:
        """Execute the data processing job."""
        import time
        import json

        print(f"DataProcessingJob {self.job_id}: Starting {self.operation} operation")

        # Simulate processing based on operation type
        if self.operation == "process":
            # Simple data processing
            time.sleep(2)
            result = {
                "job_id": self.job_id,
                "operation": self.operation,
                "processed_at": time.time(),
                "data_size": len(json.dumps(self.data)),
                "data_keys": (
                    list(self.data.keys()) if isinstance(self.data, dict) else []
                ),
                "status": "completed",
            }
        elif self.operation == "analyze":
            # Data analysis
            time.sleep(3)
            result = {
                "job_id": self.job_id,
                "operation": self.operation,
                "analyzed_at": time.time(),
                "data_analysis": {
                    "total_keys": len(self.data) if isinstance(self.data, dict) else 0,
                    "has_nested_data": (
                        any(isinstance(v, (dict, list)) for v in self.data.values())
                        if isinstance(self.data, dict)
                        else False
                    ),
                    "data_types": (
                        list(set(type(v).__name__ for v in self.data.values()))
                        if isinstance(self.data, dict)
                        else []
                    ),
                },
                "status": "completed",
            }
        elif self.operation == "transform":
            # Data transformation
            time.sleep(1)
            result = {
                "job_id": self.job_id,
                "operation": self.operation,
                "transformed_at": time.time(),
                "transformed_data": (
                    {k: str(v) for k, v in self.data.items()}
                    if isinstance(self.data, dict)
                    else str(self.data)
                ),
                "status": "completed",
            }
        else:
            raise ValueError(f"Unknown operation: {self.operation}")

        self.set_result(result)
        print(f"DataProcessingJob {self.job_id}: {self.operation} operation completed")


class FileOperationJob(QueueJobBase):
    """Example file operation job for MCP adapter."""

    def __init__(self, job_id: str, file_path: str, operation: str = "read"):
        """
        Initialize FileOperationJob.

        Args:
            job_id: Unique job identifier.
            file_path: Path to the file.
            operation: Type of file operation.
        """
        super().__init__(job_id)
        self.file_path = file_path
        self.operation = operation

    def run(self) -> None:
        """Execute the file operation job."""
        import os
        import time

        print(
            f"FileOperationJob {self.job_id}: Starting {self.operation} operation on {self.file_path}"
        )

        try:
            if self.operation == "read":
                if not os.path.exists(self.file_path):
                    raise FileNotFoundError(f"File not found: {self.file_path}")

                with open(self.file_path, "r") as f:
                    content = f.read()

                result = {
                    "job_id": self.job_id,
                    "operation": self.operation,
                    "file_path": self.file_path,
                    "file_size": len(content),
                    "content_preview": (
                        content[:100] + "..." if len(content) > 100 else content
                    ),
                    "read_at": time.time(),
                    "status": "completed",
                }

            elif self.operation == "write":
                # Create a test file
                test_content = (
                    f"Test file created by job {self.job_id} at {time.time()}"
                )
                with open(self.file_path, "w") as f:
                    f.write(test_content)

                result = {
                    "job_id": self.job_id,
                    "operation": self.operation,
                    "file_path": self.file_path,
                    "content_written": test_content,
                    "written_at": time.time(),
                    "status": "completed",
                }

            elif self.operation == "info":
                if not os.path.exists(self.file_path):
                    raise FileNotFoundError(f"File not found: {self.file_path}")

                stat = os.stat(self.file_path)
                result = {
                    "job_id": self.job_id,
                    "operation": self.operation,
                    "file_path": self.file_path,
                    "file_info": {
                        "size": stat.st_size,
                        "modified": stat.st_mtime,
                        "created": stat.st_ctime,
                        "is_file": os.path.isfile(self.file_path),
                        "is_dir": os.path.isdir(self.file_path),
                    },
                    "info_at": time.time(),
                    "status": "completed",
                }
            else:
                raise ValueError(f"Unknown file operation: {self.operation}")

            self.set_result(result)
            print(
                f"FileOperationJob {self.job_id}: {self.operation} operation completed"
            )

        except Exception as e:
            error_result = {
                "job_id": self.job_id,
                "operation": self.operation,
                "file_path": self.file_path,
                "error": str(e),
                "failed_at": time.time(),
                "status": "failed",
            }
            self.set_result(error_result)
            print(
                f"FileOperationJob {self.job_id}: {self.operation} operation failed: {e}"
            )


# MCP Commands for queue management
class QueueAddJobCommand(Command):
    """Command to add a job to the queue."""

    def __init__(self):
        super().__init__()
        self.name = "queue_add_job"
        self.description = "Add a job to the queue"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "job_type": {
                    "type": "string",
                    "enum": ["data_processing", "file_operation"],
                    "description": "Type of job to add",
                },
                "job_id": {"type": "string", "description": "Unique job identifier"},
                "params": {"type": "object", "description": "Job parameters"},
            },
            "required": ["job_type", "job_id", "params"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute queue add job command."""
        try:
            job_type = params.get("job_type")
            job_id = params.get("job_id")
            job_params = params.get("params", {})

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            # Map job types to classes
            job_classes = {
                "data_processing": DataProcessingJob,
                "file_operation": FileOperationJob,
            }

            if job_type not in job_classes:
                raise ValidationError(f"Unknown job type: {job_type}")

            await queue_system.add_job(job_classes[job_type], job_id, job_params)

            return {
                "message": f"Job {job_id} added successfully",
                "job_id": job_id,
                "job_type": job_type,
                "status": "added",
            }

        except Exception as e:
            raise MicroserviceError(f"Failed to add job: {str(e)}")


class QueueStartJobCommand(Command):
    """Command to start a job."""

    def __init__(self):
        super().__init__()
        self.name = "queue_start_job"
        self.description = "Start a job in the queue"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job identifier to start"}
            },
            "required": ["job_id"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute queue start job command."""
        try:
            job_id = params.get("job_id")

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            await queue_system.start_job(job_id)

            return {
                "message": f"Job {job_id} started successfully",
                "job_id": job_id,
                "status": "started",
            }

        except Exception as e:
            raise MicroserviceError(f"Failed to start job: {str(e)}")


class QueueStopJobCommand(Command):
    """Command to stop a job."""

    def __init__(self):
        super().__init__()
        self.name = "queue_stop_job"
        self.description = "Stop a job in the queue"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job identifier to stop"}
            },
            "required": ["job_id"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute queue stop job command."""
        try:
            job_id = params.get("job_id")

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            await queue_system.stop_job(job_id)

            return {
                "message": f"Job {job_id} stopped successfully",
                "job_id": job_id,
                "status": "stopped",
            }

        except Exception as e:
            raise MicroserviceError(f"Failed to stop job: {str(e)}")


class QueueGetJobStatusCommand(Command):
    """Command to get job status."""

    def __init__(self):
        super().__init__()
        self.name = "queue_get_job_status"
        self.description = "Get status of a job"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {
            "type": "object",
            "properties": {
                "job_id": {
                    "type": "string",
                    "description": "Job identifier to get status for",
                }
            },
            "required": ["job_id"],
        }

    async def execute(self, params: dict) -> dict:
        """Execute queue get job status command."""
        try:
            job_id = params.get("job_id")

            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            status = await queue_system.get_job_status(job_id)

            return {"job_id": job_id, "status": status}

        except Exception as e:
            raise MicroserviceError(f"Failed to get job status: {str(e)}")


class QueueListJobsCommand(Command):
    """Command to list all jobs."""

    def __init__(self):
        super().__init__()
        self.name = "queue_list_jobs"
        self.description = "List all jobs in the queue"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {"type": "object", "properties": {}}

    async def execute(self, params: dict) -> dict:
        """Execute queue list jobs command."""
        try:
            if not queue_system or not queue_system.is_running():
                raise MicroserviceError("Queue system is not running")

            jobs = await queue_system.list_jobs()

            return {"jobs": jobs, "count": len(jobs)}

        except Exception as e:
            raise MicroserviceError(f"Failed to list jobs: {str(e)}")


class QueueHealthCommand(Command):
    """Command to check queue system health."""

    def __init__(self):
        super().__init__()
        self.name = "queue_health"
        self.description = "Check queue system health"
        self.version = "1.0.0"

    def get_schema(self):
        """Get command schema."""
        return {"type": "object", "properties": {}}

    async def execute(self, params: dict) -> dict:
        """Execute queue health command."""
        try:
            is_running = queue_system.is_running() if queue_system else False

            return {
                "queue_running": is_running,
                "status": "healthy" if is_running else "unhealthy",
                "timestamp": asyncio.get_event_loop().time(),
            }

        except Exception as e:
            raise MicroserviceError(f"Failed to check queue health: {str(e)}")


async def init_queue_system():
    """Initialize the queue system."""
    global queue_system
    queue_system = AsyncQueueSystem(registry_path="/tmp/mcp_adapter_registry.jsonl")
    await queue_system.start()
    print("✅ Queue system initialized for MCP adapter")


async def cleanup_queue_system():
    """Cleanup the queue system."""
    global queue_system
    if queue_system:
        await queue_system.stop()
        print("✅ Queue system stopped for MCP adapter")


def create_mcp_app():
    """Create MCP application with queue commands."""
    app = create_app()

    # Register queue commands
    from mcp_proxy_adapter.commands.command_registry import registry

    registry.register(QueueAddJobCommand())
    registry.register(QueueStartJobCommand())
    registry.register(QueueStopJobCommand())
    registry.register(QueueGetJobStatusCommand())
    registry.register(QueueListJobsCommand())
    registry.register(QueueHealthCommand())

    # Setup startup and cleanup
    @app.on_event("startup")
    async def startup_event():
        await init_queue_system()

    @app.on_event("shutdown")
    async def shutdown_event():
        await cleanup_queue_system()

    return app


async def main():
    """Main function to run the MCP server with queue capabilities."""
    print("🚀 Starting MCP Proxy Adapter with queuemgr integration")

    # Create the app
    app = create_mcp_app()

    # Run the server
    import uvicorn

    config = uvicorn.Config(app=app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)

    print("✅ MCP server started at http://localhost:8000")
    print("📋 Available queue commands:")
    print("  - queue_add_job: Add a job to the queue")
    print("  - queue_start_job: Start a job")
    print("  - queue_stop_job: Stop a job")
    print("  - queue_get_job_status: Get job status")
    print("  - queue_list_jobs: List all jobs")
    print("  - queue_health: Check queue health")
    print()
    print("📝 Example usage:")
    print("  curl -X POST http://localhost:8000/api/v1/execute \\")
    print("    -H 'Content-Type: application/json' \\")
    print(
        '    -d \'{"command": "queue_add_job", "params": {"job_type": "data_processing", "job_id": "test1", "params": {"data": {"key": "value"}, "operation": "process"}}}\''
    )
    print()

    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
