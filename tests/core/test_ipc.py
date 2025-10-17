"""
Tests for IPC primitives.


Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

import pytest
import threading
import time
from queuemgr.core.ipc import (
    get_manager,
    create_job_shared_state,
    with_job_lock,
    update_job_state,
    read_job_state,
    set_command,
    get_command,
    clear_command,
)
from queuemgr.core.types import JobStatus, JobCommand


class TestIPCManager:
    """Test IPC manager functionality."""

    def test_get_manager(self):
        """Test getting a manager instance."""
        manager = get_manager()
        assert manager is not None
        assert hasattr(manager, "Value")
        assert hasattr(manager, "Lock")
        assert hasattr(manager, "dict")

    def test_create_job_shared_state(self):
        """Test creating shared state for a job."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        assert "status" in shared_state
        assert "command" in shared_state
        assert "progress" in shared_state
        assert "description" in shared_state
        assert "result" in shared_state
        assert "lock" in shared_state

        # Test initial values
        assert shared_state["status"].value == JobStatus.PENDING
        assert shared_state["command"].value == JobCommand.NONE
        assert shared_state["progress"].value == 0
        assert shared_state["description"].value == b""
        assert shared_state["result"].value is None


class TestJobLock:
    """Test job lock functionality."""

    def test_with_job_lock_context_manager(self):
        """Test with_job_lock context manager."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        with with_job_lock(shared_state) as state:
            assert state is shared_state
            # Lock should be acquired - try to acquire it again (should fail)
            assert not shared_state["lock"].acquire(blocking=False)

        # After context manager, lock should be released
        # Try to acquire it again (should succeed)
        assert shared_state["lock"].acquire(blocking=False)
        shared_state["lock"].release()

    def test_with_job_lock_exception_handling(self):
        """Test with_job_lock exception handling."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        with pytest.raises(ValueError, match="Shared state must contain a 'lock' key"):
            with with_job_lock({}):
                pass

        # Test that lock is released even on exception
        try:
            with with_job_lock(shared_state):
                raise ValueError("Test exception")
        except (OSError, IOError, ValueError, TimeoutError):
            pass

        # Lock should be released
        assert shared_state["lock"].acquire(blocking=False)
        shared_state["lock"].release()


class TestJobStateUpdates:
    """Test job state update functionality."""

    def test_update_job_state_single_field(self):
        """Test updating a single field."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        update_job_state(shared_state, status=JobStatus.RUNNING)

        state = read_job_state(shared_state)
        assert state["status"] == JobStatus.RUNNING
        assert state["command"] == JobCommand.NONE
        assert state["progress"] == 0

    def test_update_job_state_multiple_fields(self):
        """Test updating multiple fields."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        update_job_state(
            shared_state, status=JobStatus.RUNNING, progress=50, description="Half done"
        )

        state = read_job_state(shared_state)
        assert state["status"] == JobStatus.RUNNING
        assert state["progress"] == 50
        assert state["description"] == "Half done"

    def test_update_job_state_with_result(self):
        """Test updating state with result."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        result_data = {"key": "value", "number": 42}
        update_job_state(
            shared_state,
            status=JobStatus.COMPLETED,
            progress=100,
            description="Completed",
            result=result_data,
        )

        state = read_job_state(shared_state)
        assert state["status"] == JobStatus.COMPLETED
        assert state["progress"] == 100
        assert state["description"] == "Completed"
        assert state["result"] == result_data

    def test_update_job_state_invalid_progress(self):
        """Test updating state with invalid progress."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        with pytest.raises(ValueError, match="Progress must be between 0 and 100"):
            update_job_state(shared_state, progress=-1)

        with pytest.raises(ValueError, match="Progress must be between 0 and 100"):
            update_job_state(shared_state, progress=101)


class TestJobStateReading:
    """Test job state reading functionality."""

    def test_read_job_state_initial(self):
        """Test reading initial job state."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        state = read_job_state(shared_state)
        assert state["status"] == JobStatus.PENDING
        assert state["command"] == JobCommand.NONE
        assert state["progress"] == 0
        assert state["description"] == ""
        assert state["result"] is None

    def test_read_job_state_after_update(self):
        """Test reading job state after updates."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        # Update state
        update_job_state(
            shared_state,
            status=JobStatus.RUNNING,
            progress=75,
            description="Almost done",
        )

        state = read_job_state(shared_state)
        assert state["status"] == JobStatus.RUNNING
        assert state["progress"] == 75
        assert state["description"] == "Almost done"


class TestJobCommands:
    """Test job command functionality."""

    def test_set_command(self):
        """Test setting a command."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        set_command(shared_state, JobCommand.START)
        assert get_command(shared_state) == JobCommand.START

        set_command(shared_state, JobCommand.STOP)
        assert get_command(shared_state) == JobCommand.STOP

    def test_get_command(self):
        """Test getting a command."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        # Initial command should be NONE
        assert get_command(shared_state) == JobCommand.NONE

        # Set a command
        shared_state["command"].value = JobCommand.START
        assert get_command(shared_state) == JobCommand.START

    def test_clear_command(self):
        """Test clearing a command."""
        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        # Set a command
        set_command(shared_state, JobCommand.START)
        assert get_command(shared_state) == JobCommand.START

        # Clear the command
        clear_command(shared_state)
        assert get_command(shared_state) == JobCommand.NONE


class TestConcurrency:
    """Test concurrent access to shared state."""

    def test_concurrent_updates(self):
        """Test that concurrent updates are handled safely."""

        manager = get_manager()
        shared_state = create_job_shared_state(manager)

        results = []

        def update_worker(worker_id: int) -> None:
            """Worker function that updates shared state."""
            for i in range(10):
                update_job_state(
                    shared_state,
                    progress=i * 10,
                    description=f"Worker {worker_id} step {i}",
                )
                time.sleep(0.01)  # Small delay
            results.append(f"Worker {worker_id} completed")

        # Start multiple threads
        threads = []
        for i in range(3):
            thread = threading.Thread(target=update_worker, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All workers should have completed
        assert len(results) == 3

        # Final state should be consistent
        state = read_job_state(shared_state)
        assert 0 <= state["progress"] <= 100
        assert state["description"].startswith("Worker")
