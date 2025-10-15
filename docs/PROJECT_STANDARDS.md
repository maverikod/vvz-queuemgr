Author: Vasiliy Zdanovskiy  
email: vasilyvz@gmail.com

## Project Standards: Structure, Naming, and Placement

This document defines mandatory standards for file/folder structure and naming across code, documentation, scripts, and tests. The project is a micro-framework; therefore include rich examples under `examples/` in the code package.

### Top-Level Repository Layout
- `queuemgr/` — Python package with framework code
  - `examples/` — runnable, well-documented usage examples
  - `core/` — base types, IPC, registry, constants
  - `jobs/` — job base class and concrete jobs
  - `queue/` — job queue implementation
  - `exceptions.py` — all framework-specific exceptions and error classes
  - `constants.py` — global constants and configuration keys
- `docs/` — documentation in English only
- `scripts/` — operational and developer scripts (bash/python), non-interactive by default
- `tests/` — unit and integration tests mirroring package structure

### Naming Conventions
- Files: one class per file (except `exceptions.py`, `constants.py`)
  - Classes: `PascalCase` file names matching class name in snake_case: `JobQueue` → `job_queue.py`
  - Modules: `snake_case.py`
  - Packages: `snake_case` directories
- Documentation: `kebab-case.md` for general docs; root sections start with `##` and include short purpose paragraphs
- Scripts: `snake_case` names, `.sh` for bash, `.py` for Python
- Tests: mirror module path with `_test.py` suffix: `queuemgr/queue/job_queue.py` → `tests/queue/test_job_queue.py`

### File Headers and Docstrings
- Every code file, documentation, and project file must begin with:
  - `Author: Vasiliy Zdanovskiy` and `email: vasilyvz@gmail.com`
- Every class, property, and method must have an English docstring.
- Method docstrings must precisely describe the signature, parameter semantics, return type, errors, and side effects.

### Micro-Framework Examples
- `queuemgr/examples/` must contain:
  - Minimal example: add/start/stop/delete job
  - Error handling example: job raising exception and reporting ERROR state
  - Progress example: long-running job reporting progress
  - Registry example: reading latest snapshots
  - Each example is runnable, documented, and covered by tests where possible

### Placement Rules
- Code lives only under `queuemgr/` following one-class-per-file rule.
- `exceptions.py` contains only exception classes.
- `constants.py` contains only constants and typing literals.
- No fallback code; no hidden default behaviors. Fail fast with explicit exceptions.
- All imports at the top of files (except valid lazy-loading cases).

### Tests
- Unit tests: fast, isolated, no external side effects.
- Integration tests: spawn processes, exercise IPC, verify registry writes.
- Naming: `test_*.py`; use `pytest` with type hints and fixtures.

### Linting and Typing
- Mandatory: `black`, `flake8`, `mypy`; zero warnings/errors.
- CI should enforce formatting and typing gates.


