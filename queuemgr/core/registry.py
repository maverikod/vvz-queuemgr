"""
Registry implementations for persisting job states and results.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from .types import JobId, JobRecord, JobStatus
from .exceptions import RegistryError


class Registry(ABC):
    """Abstract registry that persists job states and results."""

    @abstractmethod
    def append(self, record: JobRecord) -> None:
        """
        Persist a new version of the job record.

        Args:
            record: JobRecord to persist.

        Raises:
            RegistryError: If persistence fails.
        """
        raise NotImplementedError

    @abstractmethod
    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """
        Return the latest record for a job, if available.

        Args:
            job_id: Job identifier to look up.

        Returns:
            Latest JobRecord for the job, or None if not found.

        Raises:
            RegistryError: If retrieval fails.
        """
        raise NotImplementedError

    @abstractmethod
    def all_latest(self) -> Iterable[JobRecord]:
        """
        Return the latest records for all jobs.

        Returns:
            Iterable of latest JobRecord for each job.

        Raises:
            RegistryError: If retrieval fails.
        """
        raise NotImplementedError


class JsonlRegistry(Registry):
    """
    Append-only JSONL registry storing JobRecord snapshots.

    Thread/process safe via file locking. Each line contains a JSON-serialized
    JobRecord. The registry maintains the latest record for each job in memory
    for fast access while persisting all historical records to disk.
    """

    def __init__(self, path: str) -> None:
        """
        Initialize the JSONL registry.

        Args:
            path: File path for the JSONL registry file.

        Raises:
            RegistryError: If the registry file cannot be created or accessed.
        """
        self.path = path
        self._latest_records: Dict[JobId, JobRecord] = {}
        self._lock = self._create_file_lock()

        # Ensure directory exists if provided
        registry_dir = os.path.dirname(path)
        if registry_dir:
            os.makedirs(registry_dir, exist_ok=True)

        # Create file if it doesn't exist
        if not os.path.exists(path):
            with open(path, "w", encoding="utf-8"):
                pass  # Create empty file

        # Load existing records
        self._load_existing_records()

    def _create_file_lock(self) -> object:  # fcntl module
        """Create a file lock for thread safety."""
        import fcntl

        return fcntl

    def _load_existing_records(self) -> None:
        """Load existing records from the JSONL file."""
        if not os.path.exists(self.path):
            return

        try:
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record_data = json.loads(line)
                        record = self._deserialize_record(record_data)
                        self._latest_records[record.job_id] = record
                    except (json.JSONDecodeError, ValueError):
                        # Skip malformed lines but continue loading
                        continue
        except IOError as e:
            raise RegistryError(f"Failed to load existing records: {e}", e)

    def _serialize_record(self, record: JobRecord) -> Dict[str, Any]:
        """Serialize a JobRecord to a dictionary for JSON storage."""
        result = {
            "job_id": record.job_id,
            "status": record.status.value,
            "progress": record.progress,
            "description": record.description,
            "result": record.result,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
        if record.started_at is not None:
            result["started_at"] = record.started_at.isoformat()
        if record.completed_at is not None:
            result["completed_at"] = record.completed_at.isoformat()
        return result

    def _deserialize_record(self, data: Dict[str, Any]) -> JobRecord:
        """Deserialize a dictionary to a JobRecord."""
        started_at = None
        if "started_at" in data and data["started_at"] is not None:
            started_at = datetime.fromisoformat(data["started_at"])

        completed_at = None
        if "completed_at" in data and data["completed_at"] is not None:
            completed_at = datetime.fromisoformat(data["completed_at"])

        return JobRecord(
            job_id=data["job_id"],
            status=JobStatus(data["status"]),
            progress=data["progress"],
            description=data["description"],
            result=data["result"],
            created_at=datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.fromisoformat(data["updated_at"]),
            started_at=started_at,
            completed_at=completed_at,
        )

    def append(self, record: JobRecord) -> None:
        """
        Persist a new version of the job record.

        Args:
            record: JobRecord to persist.

        Raises:
            RegistryError: If persistence fails.
        """
        try:
            # Serialize the record
            record_data = self._serialize_record(record)
            json_line = json.dumps(record_data, ensure_ascii=False) + "\n"

            # Write to file with file locking
            with open(self.path, "a", encoding="utf-8") as f:
                # Acquire exclusive lock
                self._lock.flock(f.fileno(), self._lock.LOCK_EX)
                try:
                    f.write(json_line)
                    f.flush()
                finally:
                    # Release lock
                    self._lock.flock(f.fileno(), self._lock.LOCK_UN)

            # Update in-memory latest records
            self._latest_records[record.job_id] = record

        except (IOError, OSError, json.JSONDecodeError) as e:
            from .exceptions import RegistryError

            raise RegistryError(f"Failed to append record: {e}", e)

    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """
        Return the latest record for a job, if available.

        Args:
            job_id: Job identifier to look up.

        Returns:
            Latest JobRecord for the job, or None if not found.
        """
        return self._latest_records.get(job_id)

    def all_latest(self) -> Iterable[JobRecord]:
        """
        Return the latest records for all jobs.

        Returns:
            Iterable of latest JobRecord for each job.
        """
        return list(self._latest_records.values())

    def get_job_history(self, job_id: JobId) -> List[JobRecord]:
        """
        Get the complete history of records for a job.

        Args:
            job_id: Job identifier to look up.

        Returns:
            List of JobRecord objects in chronological order.

        Raises:
            RegistryError: If retrieval fails.
        """
        if not os.path.exists(self.path):
            return []

        try:
            records = []
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record_data = json.loads(line)
                        if record_data.get("job_id") == job_id:
                            record = self._deserialize_record(record_data)
                            records.append(record)
                    except (json.JSONDecodeError, ValueError):
                        # Skip malformed lines
                        continue

            # Sort by updated_at timestamp
            records.sort(key=lambda r: r.updated_at)
            return records

        except IOError as e:
            from .exceptions import RegistryError

            raise RegistryError(f"Failed to get job history: {e}", e)

    def clear(self) -> None:
        """
        Clear all records from the registry.

        Raises:
            RegistryError: If clearing fails.
        """
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
            self._latest_records.clear()
        except OSError as e:
            from .exceptions import RegistryError

            raise RegistryError(f"Failed to clear registry: {e}", e)


class InMemoryRegistry(Registry):
    """
    Lightweight in-memory registry that stores only the latest job snapshots.

    Intended for unit tests and ephemeral queue instances where persistence
    is not required.
    """

    def __init__(self) -> None:
        """Initialize the in-memory storage."""
        self._latest_records: Dict[JobId, JobRecord] = {}

    def append(self, record: JobRecord) -> None:
        """Store/overwrite the latest record for the given job."""
        self._latest_records[record.job_id] = record

    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """Return the latest record for a job, if available."""
        return self._latest_records.get(job_id)

    def all_latest(self) -> Iterable[JobRecord]:
        """Iterate over the latest records for all jobs."""
        return list(self._latest_records.values())
