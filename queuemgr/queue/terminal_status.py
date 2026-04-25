"""
Terminal job status helpers for retention and API responses.

Author: Vasiliy Zdanovskiy
email: vasilyvz@gmail.com
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from queuemgr.core.types import JobStatus


TERMINAL_JOB_STATUSES: frozenset[JobStatus] = frozenset(
    {
        JobStatus.COMPLETED,
        JobStatus.ERROR,
        JobStatus.STOPPED,
        JobStatus.DELETED,
    }
)


def is_terminal_job_status(status: JobStatus) -> bool:
    """
    Return True if the status represents a finished job eligible for retention.

    Args:
        status: Current job status.

    Returns:
        True when the job is no longer active.
    """
    return status in TERMINAL_JOB_STATUSES


def derive_command_success_fields(
    result: Any, outer_status: Optional[JobStatus] = None
) -> Dict[str, Any]:
    """
    Derive explicit command-level success flags from a job result payload.

    Used when the queue reports COMPLETED but an inner command failed.

    Args:
        result: Raw job result (often a nested dict from MCP command execution).

    Returns:
        Extra fields to merge into a status dict (empty when not applicable).
    """
    inner_success, reason = _extract_inner_command_success(result)
    if outer_status in (JobStatus.STOPPED, JobStatus.DELETED):
        if inner_success is None:
            return {
                "command_success": False,
                "inner_success": False,
                "completed_with_error": True,
                "command_error_summary": "Job stopped before command completion",
            }
        if inner_success:
            return {
                "command_success": False,
                "inner_success": False,
                "completed_with_error": True,
                "command_error_summary": "Job stopped before command completion",
            }
        # Keep already-failed inner status unchanged.
        out: Dict[str, Any] = {
            "command_success": False,
            "inner_success": False,
            "completed_with_error": True,
        }
        if reason is not None:
            out["command_error_summary"] = reason
        return out

    if inner_success is None:
        return {}

    out: Dict[str, Any] = {
        "command_success": inner_success,
        "inner_success": inner_success,
        "completed_with_error": not inner_success,
    }
    if reason is not None:
        out["command_error_summary"] = reason
    return out


def _extract_inner_command_success(result: Any) -> Tuple[Optional[bool], Optional[str]]:
    """
    Inspect nested result shapes for an inner ``success`` flag.

    Returns:
        Tuple of (inner_success or None if unknown, optional short reason).
    """
    if not isinstance(result, dict):
        return None, None

    # Direct patterns
    if "success" in result and isinstance(result["success"], bool):
        msg = result.get("error") or result.get("message")
        if isinstance(msg, str):
            return result["success"], msg
        return result["success"], None

    cmd = result.get("command")
    if isinstance(cmd, dict):
        inner = cmd.get("result")
        if isinstance(inner, dict) and "success" in inner:
            succ = inner.get("success")
            if isinstance(succ, bool):
                err = inner.get("error") or inner.get("message")
                if isinstance(err, str):
                    return succ, err
                return succ, None

    mcp = result.get("mcp_result")
    if isinstance(mcp, dict) and "success" in mcp:
        succ = mcp.get("success")
        if isinstance(succ, bool):
            err = mcp.get("error") or mcp.get("message")
            if isinstance(err, str):
                return succ, err
            return succ, None

    return None, None
