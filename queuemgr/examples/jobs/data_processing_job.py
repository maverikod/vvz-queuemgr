"""
Data processing job example.

This module contains a data processing job for the full app example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import time
from typing import Dict, Any

from queuemgr.jobs.base import QueueJobBase


class DataProcessingJob(QueueJobBase):
    """Job for processing large datasets."""

    def __init__(self, job_id: str, params: Dict[str, Any]):
        """
        Initialize DataProcessingJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.data_size = params.get("data_size", 1000)
        self.batch_size = params.get("batch_size", 100)

    def execute(self) -> None:
        """Process data in batches."""
        print(
            f"DataProcessingJob {self.job_id}: Processing {self.data_size} records..."
        )

        processed = 0
        for batch in range(0, self.data_size, self.batch_size):
            # Simulate data processing
            time.sleep(0.1)
            processed += min(self.batch_size, self.data_size - batch)

            # Update progress
            progress = int((processed / self.data_size) * 100)
            print(
                f"DataProcessingJob {self.job_id}: Processed {processed}/{self.data_size} ({progress}%)"
            )

        print(
            f"DataProcessingJob {self.job_id}: Completed processing {self.data_size} records"
        )

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"DataProcessingJob {self.job_id}: Starting data processing")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"DataProcessingJob {self.job_id}: Stopping data processing")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"DataProcessingJob {self.job_id}: Data processing completed")

    def on_error(self, exc: BaseException) -> None:
        """
        Called when job encounters an error.

        Args:
            exc: The exception that occurred.
        """
        print(f"DataProcessingJob {self.job_id}: Error during processing: {exc}")
