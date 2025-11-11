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
