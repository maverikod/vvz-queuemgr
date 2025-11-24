"""
Async process runner utilities.

Contains the process entry point that powers ``AsyncProcessManager`` and the core
command loop handling control and response queues.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import logging
import signal
import time
from multiprocessing import Event, Queue
from queue import Empty
from typing import Any, Dict, Optional

from queuemgr.core.registry import JsonlRegistry
from queuemgr.process_commands import process_command
from queuemgr.process_config import ProcessManagerConfig
from queuemgr.queue.job_queue import JobQueue


LOGGER = logging.getLogger("queuemgr.async_process_manager")


def run_async_process_manager(
    control_queue: Queue,
    response_queue: Queue,
    shutdown_event: Event,
    config: ProcessManagerConfig,
) -> None:
    """
    Entry point executed in the subprocess spawned by ``AsyncProcessManager``.

    Args:
        control_queue: Queue from which commands are consumed.
        response_queue: Queue used to deliver command results.
        shutdown_event: Event signaling shutdown.
        config: Manager configuration describing registry path and limits.
    """

    def signal_handler(signum, frame):
        """Handle termination signals by setting shutdown event."""
        shutdown_event.set()

    try:
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
    except ValueError:
        # Signals can only be registered in the main thread; ignore otherwise.
        pass

    try:
        registry = JsonlRegistry(config.registry_path)
        job_queue = JobQueue(
            registry,
            max_queue_size=config.max_queue_size,
            per_job_type_limits=config.per_job_type_limits,
            completed_job_retention_seconds=config.completed_job_retention_seconds,
        )

        response_queue.put({"status": "ready"})

        cleanup_timer = time.time()
        while not shutdown_event.is_set():
            try:
                command_data: Optional[Dict[str, Any]] = None
                try:
                    command_data = control_queue.get(timeout=1.0)
                except Empty:
                    pass

                if command_data:
                    command = command_data.get("command")
                    params = command_data.get("params", {})

                    if command == "shutdown":
                        break

                    _execute_command(job_queue, command, params, response_queue)

                if time.time() - cleanup_timer > config.cleanup_interval:
                    job_queue.cleanup_completed_jobs()
                    cleanup_timer = time.time()

            except Exception as loop_error:  # pylint: disable=broad-except
                error_message = f"[manager] Command loop failed: {loop_error}"
                LOGGER.exception("Async queue manager loop failure")
                response_queue.put({"status": "error", "error": error_message})

        job_queue.shutdown()

    except Exception as error:  # pylint: disable=broad-except
        response_queue.put(
            {"status": "error", "error": f"Manager initialization failed: {error}"}
        )


def _execute_command(
    job_queue: JobQueue,
    command: str,
    params: Dict[str, Any],
    response_queue: Queue,
) -> None:
    """Execute command and push response back to response queue."""
    try:
        result = process_command(job_queue, command, params)
    except Exception as command_error:  # pylint: disable=broad-except
        error_message = f"[manager] Command '{command}' failed: {command_error}"
        LOGGER.exception("Async queue manager failed to process command '%s'", command)
        response_queue.put({"status": "error", "error": error_message})
    else:
        response_queue.put({"status": "success", "result": result})
