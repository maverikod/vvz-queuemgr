Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

# Release 1.0.9

## Summary

This release finalizes the queue persistence fixes, splits oversized modules
into dedicated helpers, and introduces an in-memory registry to make tests and
embedded scenarios simpler. All unit tests now run cleanly within the new
structure.

## Highlights

- Extracted process runners into `proc_manager_bootstrap.py` and
  `async_process_runner.py`, keeping the public managers concise.
- Added `InMemoryRegistry`, `job_registry_loader`, `job_queue_limits`, and
  `job_queue_metrics` to keep `JobQueue` within the required size limit.
- Hardened `QueueJobBase` control flow and shared-state handling; tests now
  mock `queuemgr.jobs.base_core.get_command`.
- Updated FastAPI/MCP examples and aligned tests with the new APIs.

## Testing

- `pytest` — 176 passed, 7 warnings.

