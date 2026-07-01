"""
Job used by test_consumer_global_fork_pin.py to detect fork vs spawn.

Mirrors the code_analysis consumer pattern: the *host* application pins
``multiprocessing.set_start_method("fork")`` globally before using queuemgr.
queuemgr must still start its own job processes via its private "spawn"
context (queuemgr.mp_context.get_mp_context()), regardless of the host's
global default.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import os

from queuemgr.jobs.base import QueueJobBase

from .fork_pin_marker import MARKER


class MarkerCheckJob(QueueJobBase):
    """Reports whether MARKER was inherited (fork) or empty (spawn)."""

    def execute(self) -> None:
        # Length of MARKER as observed inside the job's own process.
        marker_len = len(MARKER)
        self.set_result(
            {
                "marker_len": marker_len,
                "pid": os.getpid(),
                "inherited": marker_len > 0,
            }
        )
