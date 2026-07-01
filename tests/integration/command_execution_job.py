"""
CommandExecutionJob analogue, mirroring mcp_proxy_adapter's queue usage.

mcp_proxy_adapter enqueues jobs with params shaped like:

    {
        "command": "<command name>",
        "params": {...command-specific args...},
        "context": {...opaque JSON-ish context...},
        "auto_import_modules": ["json", "os"],
    }

The job imports the requested modules (by name, via importlib) before
dispatching the "command". This module implements a tiny in-process command
table (just enough to prove the end-to-end contract: params dict in ->
imports happen -> command executes -> set_result), it is not meant to be a
real command dispatcher.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import importlib
from typing import Any, Dict, List

from queuemgr.jobs.base import QueueJobBase


def _cmd_sum(params: Dict[str, Any]) -> Any:
    """Sum a list of numbers passed as params['numbers']."""
    numbers = params.get("numbers", [])
    return sum(numbers)


def _cmd_echo(params: Dict[str, Any]) -> Any:
    """Return params unchanged."""
    return params


_COMMAND_TABLE = {
    "sum": _cmd_sum,
    "echo": _cmd_echo,
}


class CommandExecutionJob(QueueJobBase):
    """
    Job analogue of mcp_proxy_adapter's command execution job.

    Expects self.params shaped as:
        {
            "command": str,
            "params": dict,
            "context": Optional[dict],
            "auto_import_modules": Optional[List[str]],
        }
    """

    def execute(self) -> None:
        command_name = self.params.get("command")
        command_params = self.params.get("params", {})
        auto_import_modules: List[str] = self.params.get("auto_import_modules", [])

        imported = []
        for module_name in auto_import_modules:
            module = importlib.import_module(module_name)
            imported.append(module.__name__)

        handler = _COMMAND_TABLE.get(command_name)
        if handler is None:
            self.set_result(
                {
                    "success": False,
                    "error": f"Unknown command: {command_name}",
                    "imported_modules": imported,
                }
            )
            return

        value = handler(command_params)
        self.set_result(
            {
                "success": True,
                "value": value,
                "imported_modules": imported,
            }
        )


class UnpicklableParamsJob(QueueJobBase):
    """
    Same shape as CommandExecutionJob but only used to document/pin current
    behaviour when params contain a non-picklable object (e.g. a lambda).

    Its execute() is intentionally trivial: under "spawn"/"forkserver" the
    failure happens at Process.start() time (pickling args for the target
    process), before execute() ever runs in a child. This class exists so
    the failing scenario still needs a concrete, importable job class (job
    classes must always be importable by reference for spawn to work).
    """

    def execute(self) -> None:  # pragma: no cover - not expected to run
        self.set_result({"success": True, "note": "should not reach here"})
