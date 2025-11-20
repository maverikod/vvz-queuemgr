Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

# File Split Plan

## Goal
Reduce the size of oversized modules so that each production file stays within the
350–400 line requirement while preserving single-responsibility design.

## Target Files

1. `queuemgr/proc_manager_core.py`
   - **Current state:** 407 lines with intertwined process bootstrap logic and IPC
     helpers.
   - **Plan:** Extract IPC helpers and registry bootstrap routines into a new
     `queuemgr/proc_manager_bootstrap.py`. Keep only the `ProcManager` facade and
     high-level orchestration in the original file.

2. `queuemgr/async_process_manager.py`
   - **Current state:** 475 lines combining configuration, asyncio helpers, command
     wrappers, and process bootstrap code.
   - **Plan:** Split into:
     - `async_process_runner.py` containing `_manager_process`, signal handlers,
       and registry wiring.
     - `async_process_manager.py` retaining the public `AsyncProcessManager`
       interface plus lightweight async helpers that delegate to the runner.

3. `queuemgr/queue/job_queue.py`
   - **Current state:** 497 lines containing queue orchestration, registry loading,
     eviction enforcement, and status reporting.
   - **Plan:** Create two collaborators:
     - `job_registry_loader.py` for `_load_jobs_from_registry` and placeholder job
       wiring.
     - `job_queue_limits.py` for global/per-type limit enforcement and eviction
       helpers.
     The primary `JobQueue` file will import these helpers, slimming down to core
     job lifecycle methods.

## Next Steps

- Implement the above splits incrementally, ensuring each new module follows the
  project’s docstring and typing standards.
- After each refactor, rerun `code_mapper.py` plus the full lint suite (`black`,
  `flake8`, `mypy`) to confirm compliance.

