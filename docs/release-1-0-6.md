Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com

## Queue Manager 1.0.6 Release Notes

- **JobQueue API parity** – the editable build now exports `JobQueue.list_jobs`, ensuring `queue_list_jobs` JSON-RPC calls succeed out of the box for MCP integrations and other read-only tooling.
- **Manager diagnostics** – synchronous and asyncio process managers now wrap every command execution with guarded logging/response handling, so RPC clients receive explicit errors instead of timing out when a command crashes.
- **Tooling hygiene** – added a repository-level `.flake8` configuration that skips virtual environments, caches, and logs, preventing lints from consuming tens of gigabytes of RAM on large environments.
- **Packaging** – version metadata updated to `1.0.6`, fresh wheels/sdists generated, and the release uploaded to PyPI (https://pypi.org/project/queuemgr/1.0.6/) for downstream projects such as `mcp_proxy_adapter`.

> Installation: `pip install --upgrade queuemgr`

