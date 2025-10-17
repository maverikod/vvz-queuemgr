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
        return job_queue.get_job_status(params["job_id"])

    elif command == "list_jobs":
        return job_queue.get_jobs()

    else:
        raise ValueError(f"Unknown command: {command}")
