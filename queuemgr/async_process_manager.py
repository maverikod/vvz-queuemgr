"""
AsyncIO-compatible ProcessManager for queuemgr.

This module provides asyncio-compatible versions of ProcessManager
that work correctly in asyncio applications and web servers.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import asyncio
import logging
import signal
import time
from multiprocessing import Process, Queue, Event
from queue import Empty
from typing import Dict, Any, Optional, Callable
from contextlib import asynccontextmanager

from .queue.job_queue import JobQueue
from .core.registry import JsonlRegistry
from queuemgr.core.exceptions import ProcessControlError
from .process_config import ProcessManagerConfig
from .process_commands import process_command


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
            target=self._manager_process,
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
            try:
                return self._response_queue.get(timeout=0.1)
            except:
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
        self, job_class: type, job_id: str, params: Dict[str, Any]
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
            "add_job", {"job_class": job_class, "job_id": job_id, "params": params}
        )

    async def start_job(self, job_id: str) -> None:
        """
        Start a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "start_job", "Manager is not running")

        await self._send_command_async("start_job", {"job_id": job_id})

    async def stop_job(self, job_id: str) -> None:
        """
        Stop a job.

        Args:
            job_id: Job identifier.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "stop_job", "Manager is not running")

        await self._send_command_async("stop_job", {"job_id": job_id})

    async def delete_job(self, job_id: str, force: bool = False) -> None:
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

        await self._send_command_async("delete_job", {"job_id": job_id, "force": force})

    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
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

        return await self._send_command_async("get_job_status", {"job_id": job_id})

    async def list_jobs(self) -> list:
        """
        List all jobs.

        Returns:
            List of job information.

        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("manager", "list_jobs", "Manager is not running")

        return await self._send_command_async("list_jobs", {})

    async def _send_command_async(
        self, command: str, params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Send a command to the manager process and wait for response asynchronously."""
        try:
            if self._control_queue and self._response_queue:
                # Send command in executor to avoid blocking
                loop = asyncio.get_event_loop()

                def send_command():
                    self._control_queue.put({"command": command, "params": params})

                await loop.run_in_executor(None, send_command)

                # Get response with timeout
                try:
                    response = await asyncio.wait_for(
                        self._get_response_async(), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Async manager command '%s' timed out after %.1fs",
                        command,
                        30.0,
                    )
                    raise ProcessControlError(
                        "manager", command, "Command timed out waiting for response"
                    )
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

        except asyncio.TimeoutError:
            logger.warning(
                "Async manager command '%s' exceeded response timeout", command
            )
            raise ProcessControlError(
                "manager", command, "Command timed out waiting for response"
            )
        except Exception as e:
            logger.error(
                "Async manager command '%s' failed unexpectedly: %s", command, e
            )
            raise ProcessControlError("manager", command, f"Command failed: {e}")

    @staticmethod
    def _manager_process(
        control_queue: Queue,
        response_queue: Queue,
        shutdown_event: Event,
        config: ProcessManagerConfig,
    ) -> None:
        """
        Main process function for the manager.

        Args:
            control_queue: Queue for receiving commands.
            response_queue: Queue for sending responses.
            shutdown_event: Event for shutdown signaling.
            config: Manager configuration.
        """

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            """
            Handle OS signals for graceful shutdown.

            Args:
                signum: Signal number.
                frame: Current stack frame.
            """
            shutdown_event.set()

        # Only register signals in the main thread
        try:
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
        except ValueError:
            # Signals can only be registered in the main thread
            # This is expected in subprocesses, so we ignore the error
            pass

        try:
            # Initialize the queue system
            registry = JsonlRegistry(config.registry_path)
            job_queue = JobQueue(registry)

            # Signal that we're ready
            response_queue.put({"status": "ready"})

            # Main command loop
            cleanup_timer = time.time()

            while not shutdown_event.is_set():
                try:
                    command_data: Optional[Dict[str, Any]] = None
                    try:
                        command_data = control_queue.get(timeout=1.0)
                    except Empty:
                        # No commands were received during this window.
                        pass

                    if command_data:
                        command = command_data.get("command")
                        params = command_data.get("params", {})

                        if command == "shutdown":
                            break

                        try:
                            result = process_command(job_queue, command, params)
                        except (
                            Exception
                        ) as command_error:  # pylint: disable=broad-except
                            error_message = (
                                f"[manager] Command '{command}' failed: {command_error}"
                            )
                            logger.exception(
                                "Async queue manager failed to process command '%s'",
                                command,
                            )
                            response_queue.put(
                                {"status": "error", "error": error_message}
                            )
                        else:
                            response_queue.put({"status": "success", "result": result})

                    # Periodic cleanup
                    if time.time() - cleanup_timer > config.cleanup_interval:
                        job_queue.cleanup_completed_jobs()
                        cleanup_timer = time.time()

                except Exception as loop_error:  # pylint: disable=broad-except
                    error_message = f"[manager] Command loop failed: {loop_error}"
                    logger.exception("Async queue manager loop failure")
                    response_queue.put({"status": "error", "error": error_message})

            # Graceful shutdown
            job_queue.shutdown()

        except Exception as e:
            response_queue.put(
                {"status": "error", "error": f"Manager initialization failed: {e}"}
            )


@asynccontextmanager
async def async_queue_system(
    registry_path: str = "queuemgr_registry.jsonl",
    shutdown_timeout: float = 30.0,
):
    """
    AsyncIO-compatible context manager for the queue system.

    Args:
        registry_path: Path to the registry file.
        shutdown_timeout: Timeout for graceful shutdown.

    Yields:
        AsyncProcessManager: The async process manager instance.
    """
    config = ProcessManagerConfig(
        registry_path=registry_path, shutdown_timeout=shutdown_timeout
    )
    manager = AsyncProcessManager(config)

    try:
        await manager.start()
        yield manager
    finally:
        await manager.stop()
