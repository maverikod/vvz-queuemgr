Author: Vasiliy Zdanovskiy  
email: vasilyvz@gmail.com

## AI Prompts for Contributors (Strict Rules for AI-generated Code)

Use this file verbatim as a system/developer prompt for any AI assistant generating code or docs for this project.

### Project Identity
- This repository is a Python micro-framework named `queuemgr`.
- All code, docstrings, comments, and tests must be in English.
- Country context: Ukraine.

### Critical Rules
- NO fallback code. Implement explicit behavior; fail fast with precise exceptions.
- One class = one file (exceptions: `exceptions.py`, `constants.py`).
- Files ≤ 350–400 lines; split large classes into facade + smaller units.
- Imports must be at the top of the file (except deliberate lazy loading with docstring justification).
- After writing production code, ALWAYS run `black`, `flake8`, and `mypy` and fix ALL issues.
- `JobId` MUST be UUIDv4; validate on creation and on inputs.
- Shared IPC variables MUST be read/written under a mutex.

### Structure and Placement
- Code under `queuemgr/` only, with subpackages: `core/`, `jobs/`, `queue/`, `examples/`.
- Provide rich examples under `queuemgr/examples/` with runnable scripts and clear documentation.
- Dedicated files: `queuemgr/exceptions.py` (only exceptions), `queuemgr/constants.py` (only constants).

### Documentation Requirements
- Each file begins with:
  - `Author: Vasiliy Zdanovskiy` and `email: vasilyvz@gmail.com`
- Every class, property, and method includes a precise docstring with parameter semantics and exact signatures.
- Documentation files go to `docs/` and use English.

### Coding and Naming
- Follow `docs/PROJECT_STANDARDS.md` and `docs/CODING_AND_NAMING_GUIDELINES.md` strictly.
- Naming:
  - Variables/functions: `snake_case`
  - Classes/exceptions: `PascalCase`
  - Constants: `UPPER_SNAKE_CASE`
- Concurrency:
  - Use `multiprocessing` for per-job processes.
  - Protect all shared variables with mutexes.

### Testing and Quality Gates
- Add unit and integration tests under `tests/` mirroring package structure.
- Ensure `black`, `flake8`, and `mypy` pass with zero issues before proposing edits.

### Prohibited
- No `pass` in production methods (except in abstract methods raising `NotImplementedError`).
- No broad exception swallowing; handle and re-raise with context.
- No mixing of languages in code/comments.

### Expected Deliverables from AI
- Production-ready code with imports, types, and docs.
- Separate exceptions and constants files.
- Examples in `queuemgr/examples/` that are runnable and documented.


