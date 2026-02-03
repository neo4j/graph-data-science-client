from __future__ import annotations

from abc import ABC, abstractmethod

from graphdatascience.procedure_surface.api.base_result import BaseResult


class SystemEndpoints(ABC):
    @abstractmethod
    def list_progress(
        self,
        job_id: str | None = None,
        show_completed: bool = False,
    ) -> list[ProgressResult]:
        """
        List progress of jobs.

        Parameters
        ----------
        job_id
            Identifier for the computation.
        show_completed
            Include completed jobs, by default False

        Returns
        --------
        list[ProgressResult]
            Progress of the requested job(s).
        """
        pass


class ProgressResult(BaseResult):
    username: str
    job_id: str
    task_name: str
    progress: str
    progress_bar: str
    status: str
    time_started: str
    elapsed_time: str
