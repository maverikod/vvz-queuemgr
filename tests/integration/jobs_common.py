"""
Job classes shared by queuemgr integration tests.

These must live in an importable module (not defined inside test function
bodies) because under the "spawn" start method the job class is located by
pickling a reference (module + qualified name) and re-importing it in the
child process; classes defined in a test function's local scope are not
picklable that way.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict

from queuemgr.jobs.base import QueueJobBase
from queuemgr.core.ipc_operations import get_command
from queuemgr.core.types import JobCommand


class QuickJob(QueueJobBase):
    """Trivial job that completes almost immediately."""

    def execute(self) -> None:
        self.set_result({"ok": True, "pid": os.getpid()})


class SlowJob(QueueJobBase):
    """Job that sleeps for a configurable duration, reporting its pid."""

    def execute(self) -> None:
        duration = float(self.params.get("duration", 2.0))
        self.set_result({"phase": "started", "pid": os.getpid()})
        # Sleep in small increments so shared-state writes land promptly and
        # so tests can observe the "started" phase before completion.
        remaining = duration
        step = 0.05
        while remaining > 0:
            time.sleep(min(step, remaining))
            remaining -= step
        self.set_result({"phase": "done", "pid": os.getpid(), "duration": duration})


class SleepLoopJob(QueueJobBase):
    """
    Long-running job that sleeps in a loop and cooperatively checks for a
    stop/delete command, used for terminate/stop determinism tests.

    It records its own pid in the result immediately so tests can locate the
    child process without scanning for arbitrary descendants.
    """

    def execute(self) -> None:
        self.set_result({"pid": os.getpid(), "ticks": 0})
        ticks = 0
        # Loop for a long time; the test is expected to stop/terminate us
        # well before this budget is exhausted.
        while ticks < 2000:
            if self._shared_state is not None:
                command = get_command(self._shared_state)
                if command in (JobCommand.STOP, JobCommand.DELETE):
                    return
            time.sleep(0.05)
            ticks += 1
            self.set_result({"pid": os.getpid(), "ticks": ticks})


class DirectSubclassJob(QueueJobBase):
    """
    Plain, stateless direct subclass of QueueJobBase, mirroring the vast_srv
    style of job definitions (no extra instance state beyond the base
    class's job_id/params).
    """

    def execute(self) -> None:
        numbers = self.params.get("numbers", [])
        total = sum(numbers)
        self.set_result({"sum": total, "pid": os.getpid()})
