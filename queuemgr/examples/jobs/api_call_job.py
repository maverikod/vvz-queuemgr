"""
API call job example.

This module contains an API call job for the full app example.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import requests
from typing import Dict, Any

from queuemgr.jobs.base import QueueJobBase


class ApiCallJob(QueueJobBase):
    """Job for making API calls."""

    def __init__(self, job_id: str, params: Dict[str, Any]):
        """
        Initialize ApiCallJob.

        Args:
            job_id: Unique job identifier.
            params: Job parameters.
        """
        super().__init__(job_id, params)
        self.url = params.get("url", "")
        self.method = params.get("method", "GET")
        self.headers = params.get("headers", {})
        self.data = params.get("data", None)

    def execute(self) -> None:
        """Make API call."""
        print(f"ApiCallJob {self.job_id}: Making {self.method} request to {self.url}")

        if not self.url:
            raise ValueError("URL is required")

        try:
            response = requests.request(
                method=self.method,
                url=self.url,
                headers=self.headers,
                json=self.data,
                timeout=30,
            )

            response.raise_for_status()

            result = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "data": response.json() if response.content else None,
            }

            self.set_result(result)
            print(f"ApiCallJob {self.job_id}: API call successful")

        except requests.exceptions.RequestException as e:
            raise ValueError(f"API call failed: {e}")

    def on_start(self) -> None:
        """Called when job starts."""
        print(f"ApiCallJob {self.job_id}: Starting API call")

    def on_stop(self) -> None:
        """Called when job stops."""
        print(f"ApiCallJob {self.job_id}: Stopping API call")

    def on_end(self) -> None:
        """Called when job ends."""
        print(f"ApiCallJob {self.job_id}: API call completed")

    def on_error(self, exc: BaseException) -> None:
        """
        Called when job encounters an error.

        Args:
            exc: The exception that occurred.
        """
        print(f"ApiCallJob {self.job_id}: Error during API call: {exc}")
