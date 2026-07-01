"""
Reproduces the code_analysis consumer pattern: host pins fork globally.

code_analysis.analyzer calls ``multiprocessing.set_start_method("fork")`` at
import time, before the queue is initialized. queuemgr must not be
influenced by that global default: its job (and manager) processes must
always be started through queuemgr's own private multiprocessing context
(queuemgr.mp_context.get_mp_context(), "spawn" by default / overridable via
QUEUEMGR_MP_START_METHOD), never through the ambient global default.

Because ``set_start_method`` mutates *global* interpreter state and can only
be called once per process, this scenario is run in a subprocess (see
fork_pin_helper.py) rather than in-process, so it cannot pollute the rest of
the pytest run.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _run_helper(tmp_path, env_overrides=None) -> subprocess.CompletedProcess:
    """Run fork_pin_helper.py as a subprocess and return the completed run."""
    registry_path = str(tmp_path / "registry.jsonl")
    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    return subprocess.run(
        [sys.executable, "-m", "tests.integration.fork_pin_helper", registry_path],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_job_child_uses_spawn_despite_global_fork_pin(tmp_path) -> None:
    """
    Job process must NOT inherit parent module state when the host process
    has pinned multiprocessing.set_start_method("fork") globally.

    Expected (post-fix, default QUEUEMGR_MP_START_METHOD=spawn): the job
    child re-imports fork_pin_marker fresh, so MARKER is empty in the child
    -> inherited=False, marker_len=0.

    Before the fix (job processes started via bare multiprocessing.Process,
    which honours the process-wide global default), the ambient "fork"
    pinned by this consumer wins, the child is a COW clone of the parent,
    and MARKER already contains ["parent"] -> inherited=True. That is the
    bug this test is designed to catch.
    """
    proc = _run_helper(tmp_path)

    assert proc.returncode == 0, (
        f"helper failed: returncode={proc.returncode}\n"
        f"stdout={proc.stdout}\nstderr={proc.stderr}"
    )

    verdict_lines = [
        line for line in proc.stdout.splitlines() if line.startswith("VERDICT")
    ]
    assert verdict_lines, f"No VERDICT line in stdout: {proc.stdout!r}"
    verdict = verdict_lines[-1]

    assert "inherited=False" in verdict, (
        "Job child process inherited parent module state (fork behaviour) "
        f"despite queuemgr's spawn-by-default context: {verdict}. "
        f"Full stdout: {proc.stdout}"
    )
    assert "marker_len=0" in verdict, f"Unexpected marker_len: {verdict}"


def test_job_child_honours_explicit_start_method_override(tmp_path) -> None:
    """
    Sanity check for the QUEUEMGR_MP_START_METHOD override itself: forcing
    it to "fork" must make the job child inherit MARKER (inherited=True).

    This proves the marker-detection mechanism actually works (it is not a
    false negative), and documents the escape hatch for environments where
    "spawn" is not viable.
    """
    proc = _run_helper(tmp_path, env_overrides={"QUEUEMGR_MP_START_METHOD": "fork"})

    assert proc.returncode == 0, (
        f"helper failed: returncode={proc.returncode}\n"
        f"stdout={proc.stdout}\nstderr={proc.stderr}"
    )

    verdict_lines = [
        line for line in proc.stdout.splitlines() if line.startswith("VERDICT")
    ]
    assert verdict_lines, f"No VERDICT line in stdout: {proc.stdout!r}"
    verdict = verdict_lines[-1]

    assert "inherited=True" in verdict, (
        "With QUEUEMGR_MP_START_METHOD=fork explicitly set, the job child "
        f"was expected to inherit parent state but did not: {verdict}"
    )
