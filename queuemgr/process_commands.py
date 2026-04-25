"""
Command processing for ProcessManager.

This module contains command processing logic for ProcessManager.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import Any, Dict, Optional

from .process_config import ProcessManagerConfig
from .queue.job_queue import JobQueue
from queuemgr.queue.terminal_status import derive_command_success_fields


def process_command(
    job_queue: JobQueue,
    command: str,
    params: Dict[str, Any],
    config: Optional[ProcessManagerConfig] = None,
) -> Dict[str, Any] | None:
    """
    Process a command in the manager process.

    Args:
        job_queue: The job queue instance.
        command: The command to process.
        params: Command parameters.
        config: Optional manager config (used e.g. for bounded stop_job wait).

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
        stop_timeout = config.stop_job_wait_timeout if config else 10.0
        job_queue.stop_job(params["job_id"], timeout=stop_timeout)
        return None

    elif command == "delete_job":
        job_queue.delete_job(params["job_id"], params.get("force", False))
        return None

    elif command == "get_job_status":
        job_id = params["job_id"]
        record = job_queue.get_job_status(job_id)
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
        jt = job_queue.get_job_type_name(job_id)
        if jt:
            result["job_type"] = jt
        result.update(
            derive_command_success_fields(record.result, outer_status=record.status)
        )
        return result

    elif command == "list_jobs":
        return job_queue.list_jobs(status_filter=params.get("status_filter"))

    elif command == "get_job_logs":
        logs = job_queue.get_job_logs(params["job_id"])
        return logs

    else:
        raise ValueError(f"Unknown command: {command}")
