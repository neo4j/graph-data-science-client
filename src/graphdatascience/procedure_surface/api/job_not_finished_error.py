from __future__ import annotations


class JobNotFinishedError(RuntimeError):
    """Raised when a non-blocking consumption call is made on a job that hasn't finished yet."""
