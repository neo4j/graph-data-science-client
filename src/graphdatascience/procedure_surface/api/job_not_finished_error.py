from __future__ import annotations


class JobNotFinishedError(RuntimeError):
    """Raised when a non-blocking consumption call is made on a job that hasn't finished yet."""

    def __init__(self, job_id: str):
        super().__init__(f"The Job with id {job_id} is not finished yet")
        self.job_id = job_id
