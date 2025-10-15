Author: Vasiliy Zdanovskiy  
email: vasilyvz@gmail.com

## Coding and Naming Guidelines

These rules are mandatory for all production and test code in this repository.

### Language and Style
- English-only for code, comments, and docstrings.
- Run `black`, `flake8`, and `mypy` with zero errors before each commit.
- Prefer explicitness over magic; no fallback code or implicit defaults.

### Type Hints
- Use typing everywhere; annotate all public APIs and class methods.
- Avoid `Any` except at module boundaries or for JSON payloads (document it).
- Never use unsafe casts; prefer precise types and `TypedDict`/`Protocol` where needed.

### Naming
- Variables and functions: `snake_case`
- Classes and Exceptions: `PascalCase`
- Constants and Literals: `UPPER_SNAKE_CASE`
- Private members: prefix with single underscore `_internal_var`
- Boolean names must be predicates: `is_running`, `has_result`.

### Structure and Files
- One class per file (except `exceptions.py`, `constants.py`).
- Keep files ≤ 350–400 lines; split large classes into facade + smaller collaborators.
- All imports at the top of the file; lazy imports only when justified and documented.

### Functions and Methods
- Small, cohesive, single responsibility.
- Use guard clauses; avoid deep nesting (>2-3 levels).
- No empty bodies: non-abstract methods must be implemented; abstract methods raise `NotImplementedError()`.
- Document parameter semantics, units, valid ranges, and error conditions.

### Docstrings (Required)
- Every module/file begins with Author/email header.
- Every class, property, and method must have a clear docstring including:
  - Summary in the first line.
  - Detailed behavior, preconditions, postconditions.
  - Parameters: name, type, meaning, units, valid values.
  - Returns: type and meaning.
  - Raises: exceptions with conditions.

### Concurrency and Processes
- Use `multiprocessing` for separate processes; never use threads for job execution.
- Shared variables must be accessed under a mutex.
- Do not share non-serializable objects between processes.

### Error Handling
- Fail fast; raise precise custom exceptions.
- No silent excepts; never swallow exceptions without re-raising or logging.
- Avoid broad `except Exception`; catch specific exceptions where possible.

### Testing
- Use `pytest` with type hints.
- Test names and files mirror code structure.
- Cover success paths, error paths, and race conditions for IPC.


