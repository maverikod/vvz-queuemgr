"""
IPC operations for ProcManager using /proc filesystem.

This module contains the IPC operations for communicating
with the manager process via /proc filesystem.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import time
from pathlib import Path
from typing import Dict, Any

from .exceptions import ProcessControlError


class ProcIPC:
    """IPC operations for ProcManager."""

    def __init__(self, proc_dir: Path):
        """
        Initialize ProcIPC.

        Args:
            proc_dir: Directory for /proc files.
        """
        self.proc_dir = proc_dir

    def send_command(self, command: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a command to the manager process via /proc.

        Args:
            command: Command to send.
            params: Command parameters.

        Returns:
            Response from manager process.

        Raises:
            ProcessControlError: If command fails.
        """
        try:
            # Write command to command file
            command_file = self.proc_dir / "command"
            with open(command_file, "w") as f:
                json.dump({"command": command, "params": params}, f)

            # Wait for response
            response_file = self.proc_dir / "response"
            timeout = 30.0
            start_time = time.time()

            while not response_file.exists():
                if time.time() - start_time > timeout:
                    raise ProcessControlError("manager", "command", "Command timeout")
                time.sleep(0.1)

            # Read response
            with open(response_file, "r") as f:
                response = json.load(f)

            # Clean up response file
            response_file.unlink()

            return response

        except (OSError, IOError, ValueError, TimeoutError) as e:
            raise ProcessControlError("manager", "command", f"Command failed: {e}")

    def wait_for_ready(self, timeout: float = 30.0) -> None:
        """
        Wait for manager process to be ready.

        Args:
            timeout: Maximum time to wait.

        Raises:
            ProcessControlError: If timeout exceeded.
        """
        ready_file = self.proc_dir / "ready"
        start_time = time.time()

        while not ready_file.exists():
            if time.time() - start_time > timeout:
                raise ProcessControlError(
                    "manager",
                    "initialization",
                    "Manager failed to initialize within timeout",
                )
            time.sleep(0.1)

    def is_ready(self) -> bool:
        """
        Check if manager process is ready.

        Returns:
            True if ready, False otherwise.
        """
        ready_file = self.proc_dir / "ready"
        return ready_file.exists() if ready_file else False
