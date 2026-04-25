# Release Notes: queuemgr 1.0.18

**Date**: 2026-04-25  
**Author**: Vasiliy Zdanovskiy  
**Email**: vasilyvz@gmail.com

## Summary

This release hardens queue stop lifecycle semantics to be truthful for running jobs and retained terminal metadata.

- **stopped** means execution was interrupted or cancelled before normal completion.
- **completed** means natural completion of job execution.
- **error/failed** means execution failed.
- **deleted** means a retained tombstone state.

## Lifecycle Semantics Improvements

- `stop_job()` now prioritizes truthful state transitions:
  - Requests cooperative stop first.
  - Escalates to process termination when cooperative stop times out.
  - Does not report STOPPED when the job has already naturally reached COMPLETED/ERROR.
- STOP request timestamp is recorded in queue state to track stop intent timing.
- STOPPED snapshots normalize retained result payloads so they cannot look like successful natural completion.

## API Consistency

- Command-success derivation now takes outer terminal status into account.
- For STOPPED/DELETED jobs, command-level success fields are forced non-successful to avoid contradictory outputs.

## Tests

- Added deterministic regression coverage for:
  - stopping a running long job before natural completion;
  - truthful stop behavior when job has already completed;
  - completion/error handlers not overwriting STOPPED/DELETED;
  - retained logs/status behavior for STOPPED and DELETED;
  - terminal retention coverage across COMPLETED/ERROR/STOPPED/DELETED.
