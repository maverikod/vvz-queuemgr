"""
Command processing for ProcessManager.

This module contains command processing logic for ProcessManager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Any, Dict
from .queue.job_queue import JobQueue


def process_command(
    job_queue: JobQueue, command: str, params: Dict[str, Any]
) -> Dict[str, Any] | None:
    """
    Process a command in the manager process.

    Args:
        job_queue: The job queue instance.
        command: The command to process.
        params: Command parameters.

    Returns:
        Command result.

    Raises:
        ValueError: If command is unknown.
    """
    if command == "add_job":
        job_class = params["job_class"]
        job_id = params["job_id"]
        job_params = params["params"]

        job = job_class(job_id, job_params)
        job_queue.add_job(job)
        return None

    elif command == "start_job":
        job_queue.start_job(params["job_id"])
        return None

    elif command == "stop_job":
        job_queue.stop_job(params["job_id"])
        return None

    elif command == "delete_job":
        job_queue.delete_job(params["job_id"], params.get("force", False))
        return None

    elif command == "get_job_status":
        record = job_queue.get_job_status(params["job_id"])
        # Serialize JobRecord to dict for IPC
        result = {
            "job_id": record.job_id,
            "status": record.status.name,
            "progress": record.progress,
            "description": record.description,
            "result": record.result,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
        if record.started_at is not None:
            result["started_at"] = record.started_at.isoformat()
        if record.completed_at is not None:
            result["completed_at"] = record.completed_at.isoformat()
        return result

    elif command == "list_jobs":
        return job_queue.list_jobs()

    elif command == "get_job_logs":
        logs = job_queue.get_job_logs(params["job_id"])
        return logs

    else:
        raise ValueError(f"Unknown command: {command}")
