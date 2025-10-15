"""
Tests for core types and enums.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
from datetime import datetime
from queuemgr.core.types import JobStatus, JobCommand, JobRecord


class TestJobStatus:
    """Test JobStatus enum."""

    def test_job_status_values(self):
        """Test JobStatus enum values."""
        assert JobStatus.PENDING == 0
        assert JobStatus.RUNNING == 1
        assert JobStatus.COMPLETED == 2
        assert JobStatus.ERROR == 3
        assert JobStatus.INTERRUPTED == 4

    def test_job_status_names(self):
        """Test JobStatus enum names."""
        assert JobStatus.PENDING.name == "PENDING"
        assert JobStatus.RUNNING.name == "RUNNING"
        assert JobStatus.COMPLETED.name == "COMPLETED"
        assert JobStatus.ERROR.name == "ERROR"
        assert JobStatus.INTERRUPTED.name == "INTERRUPTED"


class TestJobCommand:
    """Test JobCommand enum."""

    def test_job_command_values(self):
        """Test JobCommand enum values."""
        assert JobCommand.NONE == 0
        assert JobCommand.START == 1
        assert JobCommand.STOP == 2
        assert JobCommand.DELETE == 3

    def test_job_command_names(self):
        """Test JobCommand enum names."""
        assert JobCommand.NONE.name == "NONE"
        assert JobCommand.START.name == "START"
        assert JobCommand.STOP.name == "STOP"
        assert JobCommand.DELETE.name == "DELETE"


class TestJobRecord:
    """Test JobRecord dataclass."""

    def test_job_record_creation(self):
        """Test JobRecord creation with valid data."""
        now = datetime.now()
        record = JobRecord(
            job_id="test-job-123",
            status=JobStatus.PENDING,
            progress=0,
            description="Test job",
            result=None,
            created_at=now,
            updated_at=now,
        )

        assert record.job_id == "test-job-123"
        assert record.status == JobStatus.PENDING
        assert record.progress == 0
        assert record.description == "Test job"
        assert record.result is None
        assert record.created_at == now
        assert record.updated_at == now

    def test_job_record_immutable(self):
        """Test that JobRecord is immutable."""
        now = datetime.now()
        record = JobRecord(
            job_id="test-job-123",
            status=JobStatus.PENDING,
            progress=0,
            description="Test job",
            result=None,
            created_at=now,
            updated_at=now,
        )

        with pytest.raises(AttributeError):
            record.job_id = "new-id"

    def test_job_record_validation_job_id(self):
        """Test JobRecord validation for job_id."""
        now = datetime.now()

        # Empty job_id should raise ValueError
        with pytest.raises(ValueError, match="job_id must be a non-empty string"):
            JobRecord(
                job_id="",
                status=JobStatus.PENDING,
                progress=0,
                description="Test",
                result=None,
                created_at=now,
                updated_at=now,
            )

    def test_job_record_validation_progress(self):
        """Test JobRecord validation for progress."""
        now = datetime.now()

        # Progress < 0 should raise ValueError
        with pytest.raises(
            ValueError, match="progress must be an integer between 0 and 100"
        ):
            JobRecord(
                job_id="test-job",
                status=JobStatus.PENDING,
                progress=-1,
                description="Test",
                result=None,
                created_at=now,
                updated_at=now,
            )

        # Progress > 100 should raise ValueError
        with pytest.raises(
            ValueError, match="progress must be an integer between 0 and 100"
        ):
            JobRecord(
                job_id="test-job",
                status=JobStatus.PENDING,
                progress=101,
                description="Test",
                result=None,
                created_at=now,
                updated_at=now,
            )

    def test_job_record_validation_status(self):
        """Test JobRecord validation for status."""
        now = datetime.now()

        # Invalid status should raise ValueError
        with pytest.raises(ValueError, match="status must be a JobStatus enum value"):
            JobRecord(
                job_id="test-job",
                status="INVALID",  # type: ignore
                progress=0,
                description="Test",
                result=None,
                created_at=now,
                updated_at=now,
            )

    def test_job_record_validation_timestamps(self):
        """Test JobRecord validation for timestamps."""
        now = datetime.now()
        later = datetime.now()

        # created_at > updated_at should raise ValueError
        with pytest.raises(
            ValueError, match="created_at must be before or equal to updated_at"
        ):
            JobRecord(
                job_id="test-job",
                status=JobStatus.PENDING,
                progress=0,
                description="Test",
                result=None,
                created_at=later,
                updated_at=now,
            )

    def test_job_record_with_result(self):
        """Test JobRecord with result data."""
        now = datetime.now()
        result_data = {"key": "value", "number": 42}

        record = JobRecord(
            job_id="test-job",
            status=JobStatus.COMPLETED,
            progress=100,
            description="Completed with result",
            result=result_data,
            created_at=now,
            updated_at=now,
        )

        assert record.result == result_data
        assert record.status == JobStatus.COMPLETED
        assert record.progress == 100
