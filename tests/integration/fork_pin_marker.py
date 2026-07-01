"""
Module-level marker used to detect fork-vs-spawn inheritance for job children.

The parent process (a subprocess-launched helper script, see
``fork_pin_helper.py``) imports this module and appends "parent" to MARKER
right after import. A job's ``execute()`` then re-imports this same module
(it is already imported in the current process, so this is just a lookup)
and reports the length of MARKER:

- Under "fork": the child is a copy-on-write clone of the parent's memory,
  so MARKER already contains ["parent"] before the job code even runs ->
  length >= 1.
- Under "spawn" (or "forkserver" without this module preloaded): the child
  process re-executes the Python interpreter and re-imports modules fresh,
  so MARKER is empty at the time execute() runs -> length == 0.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from typing import List

MARKER: List[str] = []
