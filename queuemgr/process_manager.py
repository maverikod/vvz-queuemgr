"""
Process manager for the queue system.

This module provides a high-level interface for managing the entire queue system
in a separate process with automatic cleanup.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import os
import signal
import sys
import time
import threading
from multiprocessing import Process, Queue, Event
from typing import Dict, Any, Optional, Callable
from dataclasses import dataclass

from .queue.job_queue import JobQueue
from .core.registry import JsonlRegistry
from .exceptions import ProcessControlError


@dataclass
class ProcessManagerConfig:
    """Configuration for ProcessManager."""
    
    registry_path: str = "queuemgr_registry.jsonl"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    max_concurrent_jobs: int = 10


class ProcessManager:
    """
    High-level process manager for the queue system.
    
    Manages the entire queue system in a separate process with automatic
    cleanup and graceful shutdown.
    """
    
    def __init__(self, config: Optional[ProcessManagerConfig] = None):
        """
        Initialize the process manager.
        
        Args:
            config: Configuration for the process manager.
        """
        self.config = config or ProcessManagerConfig()
        self._process: Optional[Process] = None
        self._control_queue: Optional[Queue] = None
        self._response_queue: Optional[Queue] = None
        self._shutdown_event: Optional[Event] = None
        self._is_running = False
        
    def start(self) -> None:
        """
        Start the process manager in a separate process.
        
        Raises:
            ProcessControlError: If the manager is already running or fails to start.
        """
        if self._is_running:
            raise ProcessControlError("Process manager is already running")
            
        # Create communication queues
        self._control_queue = Queue()
        self._response_queue = Queue()
        self._shutdown_event = Event()
        
        # Start the manager process
        self._process = Process(
            target=self._manager_process,
            name="QueueManager",
            args=(self._control_queue, self._response_queue, self._shutdown_event, self.config)
        )
        self._process.start()
        
        # Wait for initialization
        try:
            response = self._response_queue.get(timeout=10.0)
            if response.get("status") != "ready":
                raise ProcessControlError(f"Manager failed to initialize: {response}")
        except Exception as e:
            self.stop()
            raise ProcessControlError(f"Failed to start manager: {e}")
            
        self._is_running = True
        
    def stop(self, timeout: Optional[float] = None) -> None:
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
            self._control_queue.put({"command": "shutdown"})
            
            # Wait for graceful shutdown
            self._process.join(timeout=timeout)
            
            if self._process.is_alive():
                # Force terminate if still running
                self._process.terminate()
                self._process.join(timeout=5.0)
                
                if self._process.is_alive():
                    self._process.kill()
                    self._process.join()
                    
        except Exception as e:
            # Force cleanup
            if self._process and self._process.is_alive():
                self._process.terminate()
                self._process.join(timeout=5.0)
                if self._process.is_alive():
                    self._process.kill()
                    
        finally:
            self._is_running = False
            self._process = None
            self._control_queue = None
            self._response_queue = None
            self._shutdown_event = None
            
    def is_running(self) -> bool:
        """Check if the manager is running."""
        return self._is_running and self._process is not None and self._process.is_alive()
        
    def add_job(self, job_class: type, job_id: str, params: Dict[str, Any]) -> None:
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
            raise ProcessControlError("Manager is not running")
            
        self._send_command("add_job", {
            "job_class": job_class,
            "job_id": job_id,
            "params": params
        })
        
    def start_job(self, job_id: str) -> None:
        """
        Start a job.
        
        Args:
            job_id: Job identifier.
            
        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("Manager is not running")
            
        self._send_command("start_job", {"job_id": job_id})
        
    def stop_job(self, job_id: str) -> None:
        """
        Stop a job.
        
        Args:
            job_id: Job identifier.
            
        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("Manager is not running")
            
        self._send_command("stop_job", {"job_id": job_id})
        
    def delete_job(self, job_id: str, force: bool = False) -> None:
        """
        Delete a job.
        
        Args:
            job_id: Job identifier.
            force: Force deletion even if job is running.
            
        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("Manager is not running")
            
        self._send_command("delete_job", {"job_id": job_id, "force": force})
        
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
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
            raise ProcessControlError("Manager is not running")
            
        return self._send_command("get_job_status", {"job_id": job_id})
        
    def list_jobs(self) -> list:
        """
        List all jobs.
        
        Returns:
            List of job information.
            
        Raises:
            ProcessControlError: If the manager is not running or command fails.
        """
        if not self.is_running():
            raise ProcessControlError("Manager is not running")
            
        return self._send_command("list_jobs", {})
        
    def _send_command(self, command: str, params: Dict[str, Any]) -> Any:
        """Send a command to the manager process and wait for response."""
        try:
            self._control_queue.put({"command": command, "params": params})
            response = self._response_queue.get(timeout=30.0)
            
            if response.get("status") == "error":
                raise ProcessControlError(response.get("error", "Unknown error"))
                
            return response.get("result")
            
        except Exception as e:
            raise ProcessControlError(f"Command failed: {e}")
            
    @staticmethod
    def _manager_process(control_queue: Queue, response_queue: Queue, 
                        shutdown_event: Event, config: ProcessManagerConfig) -> None:
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
            shutdown_event.set()
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
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
                    # Check for commands with timeout
                    try:
                        command_data = control_queue.get(timeout=1.0)
                        command = command_data.get("command")
                        params = command_data.get("params", {})
                        
                        if command == "shutdown":
                            break
                            
                        # Process command
                        result = ProcessManager._process_command(
                            job_queue, command, params
                        )
                        
                        response_queue.put({"status": "success", "result": result})
                        
                    except:
                        # Timeout - continue to cleanup check
                        pass
                        
                    # Periodic cleanup
                    if time.time() - cleanup_timer > config.cleanup_interval:
                        job_queue.cleanup_completed_jobs()
                        cleanup_timer = time.time()
                        
                except Exception as e:
                    response_queue.put({
                        "status": "error", 
                        "error": f"Command processing failed: {e}"
                    })
                    
            # Graceful shutdown
            job_queue.shutdown()
            
        except Exception as e:
            response_queue.put({
                "status": "error", 
                "error": f"Manager initialization failed: {e}"
            })
            
    @staticmethod
    def _process_command(job_queue: JobQueue, command: str, params: Dict[str, Any]) -> Any:
        """Process a command in the manager process."""
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


class ProcessManagerContext:
    """
    Context manager for ProcessManager.
    
    Provides automatic startup and shutdown of the process manager.
    """
    
    def __init__(self, config: Optional[ProcessManagerConfig] = None):
        """
        Initialize the context manager.
        
        Args:
            config: Configuration for the process manager.
        """
        self.manager = ProcessManager(config)
        
    def __enter__(self) -> ProcessManager:
        """Start the manager and return it."""
        self.manager.start()
        return self.manager
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the manager on exit."""
        self.manager.stop()
