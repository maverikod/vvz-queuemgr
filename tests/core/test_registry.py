"""
Tests for registry implementations.


Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import json
import os
import tempfile
import threading
import time
from datetime import datetime
from queuemgr.core.registry import JsonlRegistry
from queuemgr.core.types import JobRecord, JobStatus


class TestJsonlRegistry:
    """Test JsonlRegistry implementation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.registry_path = os.path.join(self.temp_dir, "test_registry.jsonl")
        self.registry = JsonlRegistry(self.registry_path)
    
    def teardown_method(self):
        """Clean up test fixtures."""
        if os.path.exists(self.registry_path):
            os.remove(self.registry_path)
        os.rmdir(self.temp_dir)
    
    def test_registry_initialization(self):
        """Test registry initialization."""
        assert self.registry.path == self.registry_path
        assert os.path.exists(self.registry_path)
        assert len(self.registry._latest_records) == 0
    
    def test_append_single_record(self):
        """Test appending a single record."""
        now = datetime.now()
        record = JobRecord(
            job_id="test-job-1",
            status=JobStatus.PENDING,
            progress=0,
            description="Test job",
            result=None,
            created_at=now,
            updated_at=now
        )
        
        self.registry.append(record)
        
        # Check that record was added to latest records
        assert "test-job-1" in self.registry._latest_records
        assert self.registry._latest_records["test-job-1"] == record
        
        # Check that record was written to file
        with open(self.registry_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 1
            
            data = json.loads(lines[0])
            assert data['job_id'] == "test-job-1"
            assert data['status'] == JobStatus.PENDING
            assert data['progress'] == 0
            assert data['description'] == "Test job"
            assert data['result'] is None
    
    def test_append_multiple_records(self):
        """Test appending multiple records."""
        now = datetime.now()
        
        # Append first record
        record1 = JobRecord(
            job_id="test-job-1",
            status=JobStatus.PENDING,
            progress=0,
            description="Job 1",
            result=None,
            created_at=now,
            updated_at=now
        )
        self.registry.append(record1)
        
        # Append second record
        record2 = JobRecord(
            job_id="test-job-2",
            status=JobStatus.RUNNING,
            progress=50,
            description="Job 2",
            result=None,
            created_at=now,
            updated_at=now
        )
        self.registry.append(record2)
        
        # Check latest records
        assert len(self.registry._latest_records) == 2
        assert "test-job-1" in self.registry._latest_records
        assert "test-job-2" in self.registry._latest_records
        
        # Check file content
        with open(self.registry_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
    
    def test_append_update_same_job(self):
        """Test appending updates for the same job."""
        now = datetime.now()
        
        # Initial record
        record1 = JobRecord(
            job_id="test-job-1",
            status=JobStatus.PENDING,
            progress=0,
            description="Initial",
            result=None,
            created_at=now,
            updated_at=now
        )
        self.registry.append(record1)
        
        # Updated record
        record2 = JobRecord(
            job_id="test-job-1",
            status=JobStatus.RUNNING,
            progress=100,
            description="Updated",
            result={"key": "value"},
            created_at=now,
            updated_at=now
        )
        self.registry.append(record2)
        
        # Latest record should be the updated one
        latest = self.registry.latest("test-job-1")
        assert latest is not None
        assert latest.status == JobStatus.RUNNING
        assert latest.progress == 100
        assert latest.description == "Updated"
        assert latest.result == {"key": "value"}
        
        # File should contain both records
        with open(self.registry_path, 'r') as f:
            lines = f.readlines()
            assert len(lines) == 2
    
    def test_latest_existing_job(self):
        """Test getting latest record for existing job."""
        now = datetime.now()
        record = JobRecord(
            job_id="test-job-1",
            status=JobStatus.COMPLETED,
            progress=100,
            description="Completed",
            result={"result": "success"},
            created_at=now,
            updated_at=now
        )
        
        self.registry.append(record)
        
        latest = self.registry.latest("test-job-1")
        assert latest is not None
        assert latest.job_id == "test-job-1"
        assert latest.status == JobStatus.COMPLETED
        assert latest.progress == 100
        assert latest.description == "Completed"
        assert latest.result == {"result": "success"}
    
    def test_latest_nonexistent_job(self):
        """Test getting latest record for nonexistent job."""
        latest = self.registry.latest("nonexistent-job")
        assert latest is None
    
    def test_all_latest_empty(self):
        """Test getting all latest records when registry is empty."""
        records = list(self.registry.all_latest())
        assert len(records) == 0
    
    def test_all_latest_multiple_jobs(self):
        """Test getting all latest records for multiple jobs."""
        now = datetime.now()
        
        # Add multiple jobs
        for i in range(3):
            record = JobRecord(
                job_id=f"test-job-{i}",
                status=JobStatus.COMPLETED,
                progress=100,
                description=f"Job {i}",
                result=None,
                created_at=now,
                updated_at=now
            )
            self.registry.append(record)
        
        records = list(self.registry.all_latest())
        assert len(records) == 3
        
        job_ids = {record.job_id for record in records}
        assert job_ids == {"test-job-0", "test-job-1", "test-job-2"}
    
    def test_get_job_history(self):
        """Test getting job history."""
        now = datetime.now()
        
        # Add multiple records for same job
        for i in range(3):
            record = JobRecord(
                job_id="test-job-1",
                status=JobStatus.PENDING if i == 0 else JobStatus.RUNNING if i == 1 else JobStatus.COMPLETED,
                progress=i * 50,
                description=f"Step {i}",
                result=None,
                created_at=now,
                updated_at=now
            )
            self.registry.append(record)
        
        history = self.registry.get_job_history("test-job-1")
        assert len(history) == 3
        
        # Check that records are in chronological order
        for i, record in enumerate(history):
            assert record.job_id == "test-job-1"
            assert record.progress == i * 50
    
    def test_get_job_history_nonexistent(self):
        """Test getting job history for nonexistent job."""
        history = self.registry.get_job_history("nonexistent-job")
        assert len(history) == 0
    
    def test_clear_registry(self):
        """Test clearing the registry."""
        now = datetime.now()
        
        # Add some records
        record = JobRecord(
            job_id="test-job-1",
            status=JobStatus.COMPLETED,
            progress=100,
            description="Test",
            result=None,
            created_at=now,
            updated_at=now
        )
        self.registry.append(record)
        
        assert len(self.registry._latest_records) == 1
        assert os.path.exists(self.registry_path)
        
        # Clear registry
        self.registry.clear()
        
        assert len(self.registry._latest_records) == 0
        assert not os.path.exists(self.registry_path)
    
    def test_load_existing_records(self):
        """Test loading existing records from file."""
        # Create a registry file manually
        now = datetime.now()
        record_data = {
            'job_id': 'test-job-1',
            'status': JobStatus.COMPLETED,
            'progress': 100,
            'description': 'Test job',
            'result': None,
            'created_at': now.isoformat(),
            'updated_at': now.isoformat()
        }
        
        with open(self.registry_path, 'w') as f:
            f.write(json.dumps(record_data) + '\n')
        
        # Create new registry instance
        new_registry = JsonlRegistry(self.registry_path)
        
        # Should load existing records
        assert len(new_registry._latest_records) == 1
        assert "test-job-1" in new_registry._latest_records
        
        latest = new_registry.latest("test-job-1")
        assert latest is not None
        assert latest.status == JobStatus.COMPLETED
        assert latest.progress == 100
    
    def test_malformed_json_lines(self):
        """Test handling of malformed JSON lines."""
        
        # Create a registry file with malformed lines
        now = datetime.now().isoformat()
        with open(self.registry_path, 'w') as f:
            f.write("invalid json line 1\n")
            f.write(f'{{"job_id": "test-job-1", "status": 2, "progress": 0, "description": "Test", "result": null, "created_at": "{now}", "updated_at": "{now}"}}\n')  # Valid line
            f.write("another invalid line\n")
        
        # Create new registry instance
        new_registry = JsonlRegistry(self.registry_path)
        
        # Should load only valid records
        assert len(new_registry._latest_records) == 1
        assert "test-job-1" in new_registry._latest_records
    
    def test_concurrent_access(self):
        """Test concurrent access to registry."""
        
        results = []
        
        def append_worker(worker_id: int) -> None:
            """Worker function that appends records."""
            for i in range(5):
                record = JobRecord(
                    job_id=f"worker-{worker_id}-{i}",
                    status=JobStatus.COMPLETED,
                    progress=100,
                    description=f"Worker {worker_id} record {i}",
                    result=None,
                    created_at=datetime.now(),
                    updated_at=datetime.now()
                )
                self.registry.append(record)
                time.sleep(0.01)  # Small delay
            results.append(f"Worker {worker_id} completed")
        
        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=append_worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All workers should have completed
        assert len(results) == 3
        
        # Check that all records were written
        all_records = list(self.registry.all_latest())
        assert len(all_records) == 15  # 3 workers * 5 records each
