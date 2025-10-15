Author: Vasiliy Zdanovskiy  
email: vasilyvz@gmail.com

## Technical Specification: Job Queue with Per-Job Processes and Signal Variables

### Purpose and Scope
This document specifies the architecture and APIs for a Python-based job queue system where each job runs in its own OS process, communicates via shared state protected by mutexes, and records status, progress, and results in a registry. The document provides declarative (interface-only) class descriptions and mechanisms without implementation details.

Notes:
- Communication with the product owner and team is in Russian; this documentation is in English.
- All code, docstrings, comments, and tests will be in English.
- No hardcoded logic or `pass` will be present in production code; abstract methods use `raise NotImplementedError()`.

### Non-Functional Requirements
- Python 3.10+
- Cross-platform: Linux primary target.
- Concurrency via `multiprocessing` only; each job in a dedicated child process.
- Inter-process shared state guarded by mutexes (locks) to avoid races.
- Deterministic, testable APIs; unit and integration tests can simulate job start/stop/delete and failure modes.
- Code style: black/flake8/mypy clean; type hints throughout.

### Key Concepts and Terminology
- Job Queue (ОчередьЗаданий): a coordinator managing job lifecycle and storage.
- Queue Job (ЭлементОчередиЗаданий): a base class representing work executed in a child process.
- Signals:
  - Command: Start/Stop/Delete (control plane from queue to job).
  - State: Running/Error/Completed/Interrupted (job-reported state).
  - Progress: integer percent [0..100].
  - Description: human-readable text describing current state or error.
- Registry: durable store of job states and results.

### High-Level Architecture
- The `JobQueue` maintains a dictionary of jobs by `job_id`. Each job is an instance of a subclass of `QueueJobBase`.
- For each job, the queue spawns a `multiprocessing.Process` whose target runs the job logic.
- Job and queue share an IPC structure built from `multiprocessing.Manager()` primitives:
  - Shared command variable for Start/Stop/Delete.
  - Shared state, progress, description, and result fields.
  - All writes use locks (mutexes) to ensure atomic updates.
- The queue provides APIs to add, start, stop, delete jobs, and to read statuses or list the job dictionary.
- A registry writer persists state and results changes. Minimal viable persistence: append-only JSON Lines file per queue, or pluggable registry backend interface.

### Process and Synchronization Model
- Each job process executes an event loop:
  - Checks command variable periodically or on event; reacts to Start/Stop/Delete.
  - Performs work, periodically updating progress.
  - Sets state to Running/Error/Completed/Interrupted as appropriate and writes description.
  - On exceptions, sets Error state, description to exception message, captures result if meaningful.
- All shared variables are mutated only under the job's lock. Read operations also use the lock where multi-field consistency is required.
- Queue methods that mutate job control state (e.g., issuing Stop/Delete) also acquire the same lock before updates.

### State Machine
- States: `PENDING` -> `RUNNING` -> {`COMPLETED` | `ERROR` | `INTERRUPTED`}
- Commands:
  - `START`: allowed from `PENDING` or after `INTERRUPTED` if job supports restart.
  - `STOP`: transitions `RUNNING` -> `INTERRUPTED` (graceful via `onStop`).
  - `DELETE`: allowed from any state; implies termination if still running and removal from queue.
- Progress: monotonically non-decreasing [0..100]; set to 100 only on `COMPLETED`.

### Data Model
- `JobId`: `str` (MUST be UUIDv4; enforce format on creation/validation).
- `JobStatus` (enum): `PENDING`, `RUNNING`, `COMPLETED`, `ERROR`, `INTERRUPTED`.
- `JobCommand` (enum): `NONE`, `START`, `STOP`, `DELETE`.
- `JobResult`: `Any` (JSON-serializable). For registry persistence, serialize to JSON-friendly structures.
- `JobRecord` (read model for UI/API):
  - `job_id: str`
  - `status: JobStatus`
  - `progress: int`
  - `description: str | None`
  - `result: JobResult | None`
  - `created_at: datetime`
  - `updated_at: datetime`

### IPC Primitives and Mutex Discipline
- Use `multiprocessing.Manager()` to create shared values and locks:
  - `Value("i")` for numeric progress (0..100) with lock.
  - `Value("i")` for `JobStatus` and `JobCommand` as integer codes.
  - `Manager().dict()` or `Manager().Namespace()` for textual description and result reference.
  - `Lock()` as a dedicated mutex per job to serialize updates across fields.
- Atomic Update Pattern:
  - Acquire job mutex.
  - Read-modify-write multiple shared fields.
  - Release mutex.
- Read Consistency:
  - Acquire job mutex when reading multiple fields to avoid torn reads.

### Files and Modules
- Package root: `queuemgr/`
  - `queuemgr/core/types.py`: enums and type aliases.
  - `queuemgr/core/ipc.py`: factories for Manager, shared structures, and lock discipline helpers.
  - `queuemgr/core/registry.py`: registry interface and a JSONL implementation.
  - `queuemgr/jobs/base.py`: `QueueJobBase` abstract class.
  - `queuemgr/queue/job_queue.py`: `JobQueue` implementation.
  - `queuemgr/exceptions.py`: custom exceptions (e.g., `JobNotFoundError`, `JobAlreadyExistsError`).
  - `tests/` with unit/integration tests.

### Declarative Interfaces (No Implementations)
The following class and function definitions are declarative. Implementations will follow in production code. All imports must be at the top of each file in production code.

```python
# queuemgr/core/types.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import IntEnum
from typing import Any, Dict, Optional


class JobStatus(IntEnum):
    PENDING = 0
    RUNNING = 1
    COMPLETED = 2
    ERROR = 3
    INTERRUPTED = 4


class JobCommand(IntEnum):
    NONE = 0
    START = 1
    STOP = 2
    DELETE = 3


JobId = str
JobResult = Any


@dataclass(frozen=True)
class JobRecord:
    job_id: JobId
    status: JobStatus
    progress: int
    description: Optional[str]
    result: Optional[JobResult]
    created_at: datetime
    updated_at: datetime
```

```python
# queuemgr/core/ipc.py
from __future__ import annotations
from multiprocessing import Lock, Manager
from typing import Any, Dict, Tuple


def get_manager() -> Manager:
    """Return a process-shared Manager instance for the queue runtime."""
    raise NotImplementedError


def create_job_shared_state(manager: Manager) -> Dict[str, Any]:
    """Create and return shared variables for a job (status, command, progress, description, result, mutex)."""
    raise NotImplementedError


def with_job_lock(shared_state: Dict[str, Any]) -> Any:
    """Context manager acquiring the job's mutex for consistent updates/reads across multiple fields."""
    raise NotImplementedError
```

```python
# queuemgr/core/registry.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Iterable, Optional
from .types import JobId, JobRecord


class Registry(ABC):
    """Abstract registry that persists job states and results."""

    @abstractmethod
    def append(self, record: JobRecord) -> None:
        """Persist a new version of the job record."""
        raise NotImplementedError

    @abstractmethod
    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        """Return the latest record for a job, if available."""
        raise NotImplementedError

    @abstractmethod
    def all_latest(self) -> Iterable[JobRecord]:
        """Return the latest records for all jobs."""
        raise NotImplementedError


class JsonlRegistry(Registry):
    """Append-only JSONL registry storing JobRecord snapshots. Thread/process safe via file lock."""

    def __init__(self, path: str) -> None:
        raise NotImplementedError

    def append(self, record: JobRecord) -> None:
        raise NotImplementedError

    def latest(self, job_id: JobId) -> Optional[JobRecord]:
        raise NotImplementedError

    def all_latest(self) -> Iterable[JobRecord]:
        raise NotImplementedError
```

```python
# queuemgr/jobs/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from multiprocessing import Process
from typing import Any, Dict, Optional
from ..core.types import JobCommand, JobStatus, JobId, JobResult


class QueueJobBase(ABC):
    """
    Base class for queue jobs. Each instance is executed in a dedicated process.

    Responsibilities:
    - React to Start/Stop/Delete commands from shared command variable.
    - Update shared state variables: status, progress, description, result.
    - Write snapshots to registry via the owning queue.
    """

    def __init__(self, job_id: JobId, params: Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute(self) -> None:
        """Main job logic. Runs inside the child process."""
        raise NotImplementedError

    @abstractmethod
    def on_start(self) -> None:
        """Hook invoked when the job transitions to RUNNING."""
        raise NotImplementedError

    @abstractmethod
    def on_stop(self) -> None:
        """Hook invoked on STOP command for graceful interruption."""
        raise NotImplementedError

    @abstractmethod
    def on_end(self) -> None:
        """Hook invoked when the job completes successfully (COMPLETED)."""
        raise NotImplementedError

    @abstractmethod
    def on_error(self, exc: BaseException) -> None:
        """Hook invoked when the job fails (ERROR)."""
        raise NotImplementedError

    # Process control (non-abstract; implemented in production code)
    def start_process(self) -> Process:
        """Spawn and start the child process running this job's execute loop."""
        raise NotImplementedError

    def stop_process(self, timeout: Optional[float] = None) -> None:
        """Request STOP and wait for graceful termination."""
        raise NotImplementedError

    def terminate_process(self) -> None:
        """Forcefully terminate the child process (used by DELETE)."""
        raise NotImplementedError
```

```python
# queuemgr/queue/job_queue.py
from __future__ import annotations
from datetime import datetime
from typing import Any, Dict, Mapping, Optional
from ..core.types import JobId, JobRecord, JobStatus
from ..jobs.base import QueueJobBase
from ..core.registry import Registry


class JobQueue:
    """
    Coordinator for job lifecycle and IPC state. Provides dictionary of jobs, status lookup,
    and job operations (add, delete, start, stop, suspend).
    """

    def __init__(self, registry: Registry) -> None:
        raise NotImplementedError

    # Dictionary of jobs
    def get_jobs(self) -> Mapping[JobId, QueueJobBase]:
        """Return a read-only mapping of job_id -> job instance."""
        raise NotImplementedError

    # Job status, result, description
    def get_job_status(self, job_id: JobId) -> JobRecord:
        """Return status, progress, description, and latest result for a job."""
        raise NotImplementedError

    # Job operations
    def add_job(self, job: QueueJobBase) -> JobId:
        """Add a new job; returns its job_id. Initial state is PENDING."""
        raise NotImplementedError

    def delete_job(self, job_id: JobId, force: bool = False) -> None:
        """Delete job; if running, request STOP or terminate if force=True."""
        raise NotImplementedError

    def start_job(self, job_id: JobId) -> None:
        """Start job execution in a new child process."""
        raise NotImplementedError

    def stop_job(self, job_id: JobId, timeout: Optional[float] = None) -> None:
        """Request graceful STOP and wait up to timeout."""
        raise NotImplementedError

    def suspend_job(self, job_id: JobId) -> None:
        """Optional: mark as paused (if supported)."""
        raise NotImplementedError
```

### Command Handling and Hooks
- `on_start`: allocate resources, set `status=RUNNING`, write description like "Started".
- `on_stop`: flush partial work, set `status=INTERRUPTED`, description "Stopped by user".
- `on_end`: finalize and set `status=COMPLETED`, `progress=100`, write result.
- `on_error`: cleanup and set `status=ERROR`, description with exception details.

### Registry Semantics
- Every meaningful state change appends a `JobRecord` snapshot to the registry.
- `latest(job_id)` returns the authoritative snapshot for status queries.
- The registry implementation must serialize writes (file lock) and be safe when called concurrently by the queue.

### Error Handling Strategy
- Invalid transitions raise precise exceptions (e.g., starting a RUNNING job).
- Child process failures are caught within job execution loop; state is updated to ERROR with description.
- Queue operations time out when waiting for STOP; fallback to `terminate` if requested.

### Observability
- Optional structured logging of queue and job events (start/stop/state-change).
- Metrics counters: job starts, completes, errors, interrupts; gauges: running jobs.

### Example Usage (Conceptual)
```python
from queuemgr.queue.job_queue import JobQueue
from queuemgr.core.registry import JsonlRegistry
from queuemgr.jobs.base import QueueJobBase
from typing import Any, Dict


class ExampleJob(QueueJobBase):
    def execute(self) -> None:  # implementation deferred to production code
        raise NotImplementedError

    def on_start(self) -> None:
        raise NotImplementedError

    def on_stop(self) -> None:
        raise NotImplementedError

    def on_end(self) -> None:
        raise NotImplementedError

    def on_error(self, exc: BaseException) -> None:
        raise NotImplementedError


registry = JsonlRegistry(path="runtime/registry.jsonl")
queue = JobQueue(registry=registry)
job = ExampleJob(job_id="<uuid4>", params={"n": 100})
queue.add_job(job)
queue.start_job(job.job_id)
status = queue.get_job_status(job.job_id)
```

### Acceptance Criteria
- Add/start/stop/delete job operations function correctly and update registry.
- State transitions follow the specified state machine.
- All shared variables are updated under a mutex.
- Each job runs in its own process and responds to Start/Stop/Delete.
- Progress, description, and result fields are accurately maintained.
- Linting (black, flake8) and type checking (mypy) pass with zero errors.

### Future Extensions
- Pluggable backends for the registry (SQLite, PostgreSQL).
- Remote control via REST/gRPC with authentication.
- Backpressure and scheduling policies (max concurrent jobs).
- Retry policies with exponential backoff.


