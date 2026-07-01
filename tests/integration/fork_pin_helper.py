"""
Subprocess helper script for test_consumer_global_fork_pin.py.

Simulates the code_analysis consumer: calls
``multiprocessing.set_start_method("fork")`` at import time / startup,
*before* touching queuemgr at all, then uses queuemgr's synchronous
QueueSystem API (add_job/start_job) to run a single job and waits for its
result.

Must be run as ``python -m tests.integration.fork_pin_helper <registry_path>``
(or via -c/subprocess with cwd set to the repo root) so that the relative
imports in this package resolve.

Prints exactly one line to stdout on success:
    VERDICT marker_len=<N> inherited=<True|False> pid=<child_pid>

Exits non-zero (and prints "ERROR ...") on any failure, so the calling test
can assert on both stdout content and exit code.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import multiprocessing
import sys
import time

# Pin the global start method to "fork" BEFORE importing/using queuemgr, just
# like code_analysis.analyzer does. queuemgr must not be affected by this:
# it should still start job processes via its own private context.
#
# Guarded with a try/except: queuemgr's own manager process is started via
# queuemgr's private "spawn" context (queuemgr.mp_context), which re-executes
# this file's __main__ module inside the freshly spawned manager child
# (Python's spawn bootstrap re-imports the launching script as __mp_main__).
# That reimport would hit this same module-level statement a second time,
# but the child interpreter already has its multiprocessing context fixed by
# the spawn bootstrap itself, so a second set_start_method call would raise
# RuntimeError("context has already been set"). Real consumers such as
# code_analysis only ever call this once, in their actual __main__; the
# guard here exists purely to make this reproduction script safe to
# re-import as a spawn child, and does not change the scenario under test
# (the *first* execution, in the real parent process, still pins "fork"
# globally exactly like the real consumer).
try:
    multiprocessing.set_start_method("fork")
except RuntimeError:
    pass

from queuemgr import QueueSystem  # noqa: E402  (must follow set_start_method)

from tests.integration.fork_pin_marker import MARKER  # noqa: E402
from tests.integration.fork_pin_job import MarkerCheckJob  # noqa: E402


def main() -> int:
    """Run the fork-pin scenario and print a verdict line."""
    if len(sys.argv) != 2:
        print("ERROR usage: fork_pin_helper.py <registry_path>")
        return 2

    registry_path = sys.argv[1]

    # Mark this (parent) process/module AFTER import, mirroring how a real
    # consumer's already-imported modules carry live state that a fork
    # would duplicate.
    MARKER.append("parent")

    queue = QueueSystem(registry_path=registry_path, shutdown_timeout=10.0)
    queue.start()
    try:
        queue.add_job(MarkerCheckJob, "marker-job-1", {})
        queue.start_job("marker-job-1")

        deadline = time.time() + 15.0
        status = None
        while time.time() < deadline:
            status = queue.get_job_status("marker-job-1")
            if status["status"] in ("COMPLETED", "ERROR", "STOPPED"):
                break
            time.sleep(0.05)

        if status is None or status["status"] != "COMPLETED":
            print(f"ERROR job did not complete cleanly: {status!r}")
            return 3

        result = status.get("result") or {}
        marker_len = result.get("marker_len")
        inherited = result.get("inherited")
        if marker_len is None or inherited is None:
            print(f"ERROR malformed result payload: {result!r}")
            return 4

        print(f"VERDICT marker_len={marker_len} inherited={inherited}")
        return 0
    finally:
        queue.stop()


if __name__ == "__main__":
    sys.exit(main())
