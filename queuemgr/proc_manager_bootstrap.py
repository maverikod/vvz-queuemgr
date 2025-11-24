"""
ProcManager bootstrap helpers.

This module contains helper routines used by ProcManager to bootstrap the
queue process, manage /proc IPC files, and execute incoming commands.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import json
import signal
import time
from pathlib import Path
from typing import Any, Dict

from queuemgr.core.registry import JsonlRegistry
from queuemgr.proc_config import ProcManagerConfig
from queuemgr.queue.job_queue import JobQueue


def run_proc_manager_process(config: ProcManagerConfig) -> None:
    """
    Entry point executed inside the dedicated ProcManager process.

    Args:
        config: Configuration describing registry path and /proc directory.
    """
    proc_dir = Path(config.proc_dir)
    queue: JobQueue | None = None

    try:
        signal.signal(signal.SIGTERM, lambda signum, frame: None)
        signal.signal(signal.SIGINT, lambda signum, frame: None)

        registry = JsonlRegistry(config.registry_path)
        queue = JobQueue(
            registry=registry,
            max_queue_size=getattr(config, "max_queue_size", None),
            per_job_type_limits=getattr(config, "per_job_type_limits", None),
            completed_job_retention_seconds=getattr(
                config, "completed_job_retention_seconds", None
            ),
        )

        proc_dir.mkdir(parents=True, exist_ok=True)
        (proc_dir / "ready").touch()

        _run_command_loop(proc_dir, queue)
    except (OSError, IOError, ValueError, TimeoutError):
        # Fail silently; the parent process handles escalation.
        pass
    finally:
        _cleanup_proc_dir(proc_dir)
        if queue:
            try:
                queue.shutdown()
            except Exception:
                pass


def _run_command_loop(proc_dir: Path, queue: JobQueue) -> None:
    """Continuously poll for commands and dispatch them to the queue."""
    command_file = proc_dir / "command"
    response_file = proc_dir / "response"

    while True:
        try:
            if command_file.exists():
                with open(command_file, "r", encoding="utf-8") as handle:
                    command_data = json.load(handle)

                response = process_proc_command(
                    queue=queue,
                    command=command_data["command"],
                    params=command_data["params"],
                )

                with open(response_file, "w", encoding="utf-8") as handle:
                    json.dump(response, handle)

                command_file.unlink(missing_ok=True)

            time.sleep(0.1)
        except (OSError, IOError, ValueError, TimeoutError):
            break


def process_proc_command(
    queue: JobQueue, command: str, params: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single IPC command for the /proc-based manager.

    Args:
        queue: Active JobQueue instance.
        command: Command verb to execute.
        params: Serialized command payload.

    Returns:
        Dictionary describing command status and optional result payload.
    """
    try:
        if command == "add_job":
            job_class_name = params["job_class_name"]
            job_class_module = params["job_class_module"]
            module = __import__(job_class_module, fromlist=[job_class_name])
            job_class = getattr(module, job_class_name)
            queue.add_job(job_class, params["job_id"], params["params"])
            return {"status": "success"}

        if command == "start_job":
            queue.start_job(params["job_id"])
            return {"status": "success"}

        if command == "stop_job":
            queue.stop_job(params["job_id"])
            return {"status": "success"}

        if command == "delete_job":
            queue.delete_job(params["job_id"])
            return {"status": "success"}

        if command == "get_job_status":
            status = queue.get_job_status(params["job_id"])
            # Serialize JobRecord to dict
            result = {
                "job_id": status.job_id,
                "status": status.status.name,
                "progress": status.progress,
                "description": status.description,
                "result": status.result,
                "created_at": status.created_at.isoformat(),
                "updated_at": status.updated_at.isoformat(),
            }
            if status.started_at is not None:
                result["started_at"] = status.started_at.isoformat()
            if status.completed_at is not None:
                result["completed_at"] = status.completed_at.isoformat()
            return {"status": "success", "result": result}

        if command == "list_jobs":
            jobs = queue.list_jobs()
            return {"status": "success", "result": jobs}

        if command == "get_job_logs":
            logs = queue.get_job_logs(params["job_id"])
            return {"status": "success", "result": logs}

        if command == "shutdown":
            queue.shutdown()
            return {"status": "shutdown"}

        return {"status": "error", "error": f"Unknown command: {command}"}
    except (OSError, IOError, ValueError, TimeoutError) as error:
        return {"status": "error", "error": str(error)}


def _cleanup_proc_dir(proc_dir: Path) -> None:
    """Remove /proc IPC files created by the manager process."""
    try:
        if proc_dir.exists():
            for file in proc_dir.glob("*"):
                try:
                    file.unlink()
                except FileNotFoundError:
                    continue
            proc_dir.rmdir()
    except (OSError, IOError):
        pass
