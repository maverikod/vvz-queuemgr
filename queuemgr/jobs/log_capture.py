"""
Log capture functionality for queue jobs.

This module provides utilities for capturing stdout/stderr output
from job processes and storing it in shared state for IPC.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from typing import List


class LogCapture:
    """
    Capture stdout/stderr output and write to shared list.

    This class acts as a file-like object that writes to a shared list
    for inter-process communication of job output.
    """

    def __init__(self, shared_list: List[str]) -> None:
        """
        Initialize log capture.

        Args:
            shared_list: Shared list to append log lines to.
        """
        self._shared_list = shared_list
        self._buffer = ""

    def write(self, text: str) -> int:
        """
        Write text to shared list, splitting on newlines.

        Args:
            text: Text to write.

        Returns:
            Number of characters written.
        """
        if not text:
            return 0

        self._buffer += text
        lines = self._buffer.split("\n")
        # Keep incomplete line in buffer
        self._buffer = lines[-1]
        # Add complete lines to shared list
        for line in lines[:-1]:
            if line:  # Skip empty lines
                self._shared_list.append(line)

        return len(text)

    def flush(self) -> None:
        """Flush any remaining buffer."""
        if self._buffer:
            self._shared_list.append(self._buffer)
            self._buffer = ""
