"""
Process manager using /proc filesystem for Linux.

This module provides a high-level interface for managing the entire queue system
using Linux /proc filesystem for inter-process communication.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import os
import signal
import sys
import time
import threading
import tempfile
from multiprocessing import Process, Event
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass
from pathlib import Path

from .queue.job_queue import JobQueue
from .core.registry import JsonlRegistry
from .exceptions import ProcessControlError


@dataclass
class ProcManagerConfig:
    """Configuration for ProcManager."""
    
    registry_path: str = "queuemgr_registry.jsonl"
    proc_dir: str = "/tmp/queuemgr"
    shutdown_timeout: float = 30.0
    cleanup_interval: float = 60.0
    max_concurrent_jobs: int = 10


class ProcManager:
    """
    High-level process manager using /proc filesystem.
    
    Manages the entire queue system in a separate process with automatic
    cleanup and graceful shutdown using Linux /proc filesystem.
    """
    
    def __init__(self, config: Optional[ProcManagerConfig] = None):
        """
        Initialize the process manager.
        
        Args:
            config: Configuration for the process manager.
        """
        self.config = config or ProcManagerConfig()
        self._process: Optional[Process] = None
        self._proc_dir: Optional[Path] = None
        self._is_running = False
        
    def start(self) -> None:
        """
        Start the process manager in a separate process.
        
        Raises:
            ProcessControlError: If the manager is already running or fails to start.
        """
        if self._is_running:
            raise ProcessControlError("manager", "start", Exception("Process manager is already running"))
            
        # Create proc directory
        self._proc_dir = Path(self.config.proc_dir)
        self._proc_dir.mkdir(parents=True, exist_ok=True)
        
        # Start the manager process
        self._process = Process(
            target=self._manager_process,
            name="QueueManager",
            args=(self.config,)
        )
        self._process.start()
        
        # Wait for initialization
        try:
            self._wait_for_ready()
        except Exception as e:
            self.stop()
            raise ProcessControlError("manager", "start", e)
            
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
            self._send_command("shutdown", {})
            
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
            # Clean up proc directory
            if self._proc_dir and self._proc_dir.exists():
                import shutil
                shutil.rmtree(self._proc_dir, ignore_errors=True)
            self._proc_dir = None
            
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
        self._send_command("add_job", {
            "job_class_name": job_class.__name__,
            "job_class_module": job_class.__module__,
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
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
            raise ProcessControlError("manager", "operation", Exception("Manager is not running"))
            
        return self._send_command("list_jobs", {})
        
    def _wait_for_ready(self, timeout: float = 10.0) -> None:
        """Wait for the manager process to be ready."""
        start_time = time.time()
        ready_file = self._proc_dir / "ready"
        
        while time.time() - start_time < timeout:
            if ready_file.exists():
                return
            time.sleep(0.1)
            
        raise ProcessControlError("manager", "initialization", Exception("Manager failed to initialize within timeout"))
        
    def _send_command(self, command: str, params: Dict[str, Any]) -> Any:
        """Send a command to the manager process via /proc."""
        try:
            # Write command to command file
            command_file = self._proc_dir / "command"
            with open(command_file, "w") as f:
                json.dump({"command": command, "params": params}, f)
                
            # Wait for response
            response_file = self._proc_dir / "response"
            start_time = time.time()
            
            while time.time() - start_time < 30.0:
                if response_file.exists():
                    with open(response_file, "r") as f:
                        response = json.load(f)
                    
                    # Remove response file
                    response_file.unlink()
                    
                    if response.get("status") == "error":
                        raise ProcessControlError("manager", "command", Exception(response.get("error", "Unknown error")))
                        
                    return response.get("result")
                    
                time.sleep(0.1)
                
            raise ProcessControlError("manager", "command", Exception("Command timeout"))
            
        except Exception as e:
            raise ProcessControlError("manager", "command", e)
            
    @staticmethod
    def _manager_process(config: ProcManagerConfig) -> None:
        """
        Main process function for the manager.
        
        Args:
            config: Manager configuration.
        """
        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            # Write shutdown signal to proc
            shutdown_file = Path(config.proc_dir) / "shutdown"
            shutdown_file.touch()
            
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            # Create proc directory
            proc_dir = Path(config.proc_dir)
            proc_dir.mkdir(parents=True, exist_ok=True)
            
            # Initialize the queue system
            registry = JsonlRegistry(config.registry_path)
            job_queue = JobQueue(registry)
            
            # Signal that we're ready
            ready_file = proc_dir / "ready"
            ready_file.touch()
            print(f"Manager process ready: {ready_file}")
            
            # Main command loop
            cleanup_timer = time.time()
            command_file = proc_dir / "command"
            response_file = proc_dir / "response"
            shutdown_file = proc_dir / "shutdown"
            
            while True:
                try:
                    # Check for shutdown signal
                    if shutdown_file.exists():
                        break
                        
                    # Check for commands
                    if command_file.exists():
                        # Read command
                        with open(command_file, "r") as f:
                            command_data = json.load(f)
                            
                        command = command_data.get("command")
                        params = command_data.get("params", {})
                        
                        # Remove command file
                        command_file.unlink()
                        
                        if command == "shutdown":
                            break
                            
                        # Process command
                        try:
                            result = ProcManager._process_command(
                                job_queue, command, params
                            )
                            
                            # Write response
                            with open(response_file, "w") as f:
                                json.dump({"status": "success", "result": result}, f)
                                
                        except Exception as e:
                            # Write error response
                            with open(response_file, "w") as f:
                                json.dump({
                                    "status": "error", 
                                    "error": str(e)
                                }, f)
                                
                    # Periodic cleanup
                    if time.time() - cleanup_timer > config.cleanup_interval:
                        job_queue.cleanup_completed_jobs()
                        cleanup_timer = time.time()
                        
                    # Small delay to prevent busy waiting
                    time.sleep(0.1)
                    
                except Exception as e:
                    # Write error response
                    with open(response_file, "w") as f:
                        json.dump({
                            "status": "error", 
                            "error": f"Command processing failed: {e}"
                        }, f)
                        
            # Graceful shutdown
            job_queue.shutdown()
            
        except Exception as e:
            # Write error to proc
            error_file = Path(config.proc_dir) / "error"
            with open(error_file, "w") as f:
                f.write(f"Manager initialization failed: {e}")
                
    @staticmethod
    def _process_command(job_queue: JobQueue, command: str, params: Dict[str, Any]) -> Any:
        """Process a command in the manager process."""
        if command == "add_job":
            job_class_name = params["job_class_name"]
            job_class_module = params["job_class_module"]
            job_id = params["job_id"]
            job_params = params["params"]
            
            # Import the job class
            import importlib
            module = importlib.import_module(job_class_module)
            job_class = getattr(module, job_class_name)
            
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
            job_record = job_queue.get_job_status(params["job_id"])
            return {
                "job_id": job_record.job_id,
                "status": job_record.status,
                "progress": job_record.progress,
                "description": job_record.description,
                "result": job_record.result,
                "created_at": job_record.created_at.isoformat() if job_record.created_at else None,
                "updated_at": job_record.updated_at.isoformat() if job_record.updated_at else None
            }
            
        elif command == "list_jobs":
            jobs = job_queue.get_jobs()
            result = []
            for job_id, job in jobs.items():
                status_data = job.get_status()
                result.append({
                    "job_id": job_id,
                    "status": status_data["status"],
                    "progress": status_data["progress"],
                    "description": status_data["description"],
                    "result": status_data["result"]
                })
            return result
            
        else:
            raise ValueError(f"Unknown command: {command}")


class ProcManagerContext:
    """
    Context manager for ProcManager.
    
    Provides automatic startup and shutdown of the process manager.
    """
    
    def __init__(self, config: Optional[ProcManagerConfig] = None):
        """
        Initialize the context manager.
        
        Args:
            config: Configuration for the process manager.
        """
        self.manager = ProcManager(config)
        
    def __enter__(self) -> ProcManager:
        """Start the manager and return it."""
        self.manager.start()
        return self.manager
        
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Stop the manager on exit."""
        self.manager.stop()
