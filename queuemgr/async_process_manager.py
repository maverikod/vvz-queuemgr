"""
AsyncIO-compatible ProcessManager for queuemgr.

This module provides asyncio-compatible versions of ProcessManager
that work correctly in asyncio applications and web servers.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import logging
import time
from multiprocessing import Process, Queue, Event
from typing import Dict, Any, Optional, Callable, List
from contextlib import asynccontextmanager

from queuemgr.core.exceptions import ProcessControlError
from .process_config import ProcessManagerConfig
from .async_process_runner import run_async_process_manager


logger = logging.getLogger("queuemgr.async_process_manager")


class AsyncProcessManager:
    """
    AsyncIO-compatible process manager for the queue system.

    Manages the entire queue system in a separate process with automatic
    cleanup and graceful shutdown, designed to work with asyncio applications.
    """

    def __init__(self, config: Optional[ProcessManagerConfig] = None):
        """
        Initialize the async process manager.

        Args:
            config: Configuration for the process manager.
        """
        self.config = config or ProcessManagerConfig()
        self._process: Optional[Process] = None
        self._control_queue: Optional[Queue] = None
        self._response_queue: Optional[Queue] = None
        self._shutdown_event: Optional[Event] = None
        self._is_running = False
        self._shutdown_callback: Optional[Callable] = None

    async def start(self) -> None:
        """
        Start the process manager in a separate process.

        Raises:
            ProcessControlError: If the manager is already running or fails to start.
        """
        if self._is_running:
            raise ProcessControlError(
                "manager", "start", "Process manager is already running"
            )

        # Create communication queues
        self._control_queue = Queue()
        self._response_queue = Queue()
        self._shutdown_event = Event()

        # Start the manager process
        self._process = Process(
            target=run_async_process_manager,
            name="AsyncQueueManager",
            args=(
                self._control_queue,
                self._response_queue,
                self._shutdown_event,
                self.config,
            ),
        )
        self._process.start()

        # Wait for initialization with asyncio timeout
        try:
            # Use asyncio.wait_for for timeout handling
            response = await asyncio.wait_for(self._get_response_async(), timeout=10.0)
            if response.get("status") != "ready":
                raise ProcessControlError(
                    "manager", "start", f"Manager failed to initialize: {response}"
                )
        except asyncio.TimeoutError:
            await self.stop()
            raise ProcessControlError(
                "manager", "start", "Manager initialization timed out"
            )
        except Exception as e:
            await self.stop()
            raise ProcessControlError(
                "manager", "start", f"Failed to start manager: {e}"
            )

        self._is_running = True

    async def stop(self, timeout: Optional[float] = None) -> None:
        """
        Stop the process manager and all running jobs.

        Args:
            timeout: Maximum time to wait for graceful shutdown.
        """
        if not self._is_running:
            return

        timeout = timeout or self.config.shutdown_timeout

        try:
            # Send shutdown command
            if self._control_queue:
                self._control_queue.put({"command": "shutdown"})

            # Wait for graceful shutdown with asyncio
            if self._process:
                await asyncio.wait_for(
                    self._wait_for_process_shutdown(timeout), timeout=timeout
                )

        except asyncio.TimeoutError:
            # Force terminate if still running
            if self._process and self._process.is_alive():
                self._process.terminate()
                await asyncio.sleep(0.1)  # Brief wait
                if self._process.is_alive():
                    self._process.kill()
                    await asyncio.sleep(0.1)

        except Exception:
            # Force cleanup
            if self._process and self._process.is_alive():
                self._process.terminate()
                await asyncio.sleep(0.1)
                if self._process.is_alive():
                    self._process.kill()

        finally:
            self._is_running = False
            self._process = None
            self._control_queue = None
            self._response_queue = None
            self._shutdown_event = None

    async def _wait_for_process_shutdown(self, timeout: float) -> None:
        """Wait for process shutdown with asyncio."""
        start_time = time.time()
        while self._process and self._process.is_alive():
            if time.time() - start_time > timeout:
                break
            await asyncio.sleep(0.1)

    async def _get_response_async(self) -> Dict[str, Any]:
        """Get response from queue asynchronously."""
        loop = asyncio.get_event_loop()

        def get_response():
            """Attempt to read a single message from the response queue."""
            try:
                return self._response_queue.get(timeout=0.1)
            except Exception:
                return None

        # Poll the queue with short timeouts to avoid blocking
        for _ in range(100):  # 10 seconds total
            result = await loop.run_in_executor(None, get_response)
            if result is not None:
                return result
            await asyncio.sleep(0.1)

        raise asyncio.TimeoutError("No response received")

    def is_running(self) -> bool:
        """Check if the manager is running."""
        return (
            self._is_running and self._process is not None and self._process.is_alive()
        )

    async def add_job(
        self,
        job_class: type,
        job_id: str,
        params: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> None:
        """
        Add a job to the queue.

        Args:
            job_class: Job class to instantiate.
            job_id: Unique job identifier.
            params: Job parameters.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "add_job", "Manager is not running")

        await self._send_command_async(
            "add_job",
            {"job_class": job_class, "job_id": job_id, "params": params},
            timeout=timeout,
        )

    async def start_job(self, job_id: str, timeout: Optional[float] = None) -> None:
        """
        Start a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "start_job", "Manager is not running")

        await self._send_command_async("start_job", {"job_id": job_id}, timeout=timeout)

    async def stop_job(self, job_id: str, timeout: Optional[float] = None) -> None:
        """
        Stop a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "stop_job", "Manager is not running")

        await self._send_command_async("stop_job", {"job_id": job_id}, timeout=timeout)

    async def delete_job(
        self, job_id: str, force: bool = False, timeout: Optional[float] = None
    ) -> None:
        """
        Delete a job.

        Args:
            job_id: Job identifier.
            force: Force deletion even if job is running.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "delete_job", "Manager is not running")

        await self._send_command_async(
            "delete_job", {"job_id": job_id, "force": force}, timeout=timeout
        )

    async def get_job_status(
        self, job_id: str, timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job identifier.

        Returns:
            Job status information.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "manager", "get_job_status", "Manager is not running"
            )

        return await self._send_command_async(
            "get_job_status", {"job_id": job_id}, timeout=timeout
        )

    async def list_jobs(self, timeout: Optional[float] = None) -> list:
        """
        List all jobs.

        Returns:
            List of job information.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "list_jobs", "Manager is not running")

        return await self._send_command_async("list_jobs", {}, timeout=timeout)

    async def get_job_logs(
        self, job_id: str, timeout: Optional[float] = None
    ) -> Dict[str, List[str]]:
        """
        Get stdout and stderr logs for a job.

        Args:
            job_id: Job identifier.

        Returns:
            Dictionary containing stdout and stderr log lines.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError(
                "manager", "get_job_logs", "Manager is not running"
            )

        return await self._send_command_async(
            "get_job_logs", {"job_id": job_id}, timeout=timeout
        )

    async def _send_command_async(
        self, command: str, params: Dict[str, Any], timeout: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Send a command to the manager process and wait for response asynchronously.

        The timeout applies to the control-plane round trip only and does not
        limit the execution time of individual jobs.
        """
        effective_timeout = (
            timeout if timeout is not None else self.config.command_timeout
        )

        try:
            if self._control_queue and self._response_queue:
                # Send command in executor to avoid blocking the event loop.
                loop = asyncio.get_event_loop()

                def send_command() -> None:
                    """Put the serialized command into the control queue."""
                    self._control_queue.put({"command": command, "params": params})

                await loop.run_in_executor(None, send_command)

                # Get response with timeout
                try:
                    response = await asyncio.wait_for(
                        self._get_response_async(), timeout=effective_timeout
                    )
                except asyncio.TimeoutError as exc:
                    logger.warning(
                        "Async manager command '%s' timed out after %.1fs",
                        command,
                        effective_timeout,
                    )
                    raise ProcessControlError(
                        "manager", command, "Command timed out waiting for response"
                    ) from exc
            else:
                raise ProcessControlError("manager", command, "Queues not initialized")

            if response.get("status") == "error":
                error_message = response.get("error", "Unknown error")
                logger.error(
                    "Async manager command '%s' failed inside manager: %s",
                    command,
                    error_message,
                )
                raise ProcessControlError("manager", command, error_message)

            return response.get("result")

        except asyncio.TimeoutError as exc:
            logger.warning(
                "Async manager command '%s' exceeded response timeout", command
            )
            raise ProcessControlError(
                "manager", command, "Command timed out waiting for response"
            ) from exc
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(
                "Async manager command '%s' failed unexpectedly: %s", command, exc
            )
            raise ProcessControlError("manager", command, f"Command failed: {exc}")


@asynccontextmanager
async def async_queue_system(
    registry_path: str = "queuemgr_registry.jsonl",
    shutdown_timeout: float = 30.0,
    command_timeout: float = 30.0,
):
    """
    AsyncIO-compatible context manager for the queue system.

    Args:
        registry_path: Path to the registry file.
        shutdown_timeout: Timeout for graceful shutdown.
        command_timeout: Maximum time to wait for a manager control command
            response. This timeout applies only to IPC control operations and
            does not limit job execution time.

    Yields:
        AsyncProcessManager: The async process manager instance.
    """
    config = ProcessManagerConfig(
        registry_path=registry_path,
        shutdown_timeout=shutdown_timeout,
        command_timeout=command_timeout,
    )
    manager = AsyncProcessManager(config)

    try:
        await manager.start()
        yield manager
    finally:
        await manager.stop()
