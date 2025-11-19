# Release 1.0.8

**Author**: Vasiliy Zdanovskiy  
**Email**: vasilyvz@gmail.com  
**Date**: 2025-01-27

## Summary

Release 1.0.8 fixes critical bug where jobs were not loaded from registry file on startup, making persistent job storage non-functional.

## Bug Fixes

### Registry Loading on Startup

**Problem:** `AsyncQueueSystem` and `JobQueue` did not load existing jobs from registry file when `start()` was called. Jobs written to registry were not accessible after restart.

**Solution:**
- Added automatic loading of jobs from registry in `JobQueue.__init__()`
- Created `RegistryPlaceholderJob` class for restoring job metadata from registry records
- Jobs are now automatically loaded from registry file on startup
- Jobs are accessible via `list_jobs()` and `get_job_status()` after restart

**Impact:**
- Jobs are now persistent across restarts
- Registry file functionality is fully operational
- Jobs can be recovered after server restart

## New Components

### `RegistryPlaceholderJob`

New placeholder job class that:
- Restores job metadata from registry records
- Provides read-only access to job status, progress, and results
- Cannot be executed (requires original job class)
- Used automatically when loading jobs from registry

## Implementation Details

- `JobQueue._load_jobs_from_registry()` - Automatically called during initialization
- Loads all jobs from `registry.all_latest()`
- Creates placeholder jobs for each registry record
- Preserves job metadata (status, progress, description, result, timestamps)

## API Changes

### `JobQueue.__init__()`

Now automatically loads jobs from registry during initialization. No API changes required.

### `process_commands.py`

Fixed serialization of `JobRecord` to dictionary for IPC communication:
- `get_job_status` now properly serializes `JobRecord` to dict

## Testing

Added comprehensive test suite in `tests/queue/test_job_queue_registry_loading.py`:
- 6 test cases covering registry loading scenarios
- Tests for empty registry, file registry, placeholder jobs, and integration

## Backward Compatibility

Fully backward compatible. Existing code continues to work without changes.

## Files Changed

- `queuemgr/jobs/registry_job.py` - New placeholder job class
- `queuemgr/queue/job_queue.py` - Added registry loading logic
- `queuemgr/process_commands.py` - Fixed JobRecord serialization
- `queuemgr/jobs/__init__.py` - Export RegistryPlaceholderJob
- `tests/queue/test_job_queue_registry_loading.py` - New test suite

## Related Issues

Fixes bug where jobs were not accessible after restart, making persistent storage unusable.

