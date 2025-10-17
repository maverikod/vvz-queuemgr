"""
File operation job example.

This module contains a file operation job for the full app example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import shutil
from pathlib import Path
from typing import Dict, Any

from queuemgr.jobs.base import QueueJobBase


class FileOperationJob(QueueJobBase):
    """Job for file operations like copying, moving, or processing files."""

    def __init__(self, job_id: str, params: Dict[str, Any]):
        """
        Initialize FileOperationJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.source_path = params.get("source_path", "")
        self.destination_path = params.get("destination_path", "")
        self.operation = params.get("operation", "copy")

    def execute(self) -> None:
        """Perform file operation."""
        print(f"FileOperationJob {self.job_id}: {self.operation} file operation")

        if not self.source_path or not self.destination_path:
            raise ValueError("Source and destination paths are required")

        source = Path(self.source_path)
        destination = Path(self.destination_path)

        if not source.exists():
            raise FileNotFoundError(f"Source file not found: {self.source_path}")

        # Create destination directory if needed
        destination.parent.mkdir(parents=True, exist_ok=True)

        if self.operation == "copy":
            shutil.copy2(source, destination)
            print(f"FileOperationJob {self.job_id}: Copied {source} to {destination}")
        elif self.operation == "move":
            shutil.move(str(source), str(destination))
            print(f"FileOperationJob {self.job_id}: Moved {source} to {destination}")
        else:
            raise ValueError(f"Unsupported operation: {self.operation}")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"FileOperationJob {self.job_id}: Starting file operation")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"FileOperationJob {self.job_id}: Stopping file operation")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"FileOperationJob {self.job_id}: File operation completed")

    def on_error(self, exc: BaseException) -> None:
        """
        Called when job encounters an error.

        Args:
            exc: The exception that occurred.
        """
        print(f"FileOperationJob {self.job_id}: Error during file operation: {exc}")
