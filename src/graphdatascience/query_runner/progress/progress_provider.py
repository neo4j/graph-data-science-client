from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class TaskWithProgress:
    task_name: str
    progress_percent: str
    status: str
    sub_tasks_description: str | None = None

    def relative_progress(self) -> float | None:
        try:
            return float(self.progress_percent.removesuffix("%"))
        except ValueError:
            return None


class ProgressProvider(ABC):
    @abstractmethod
    def root_task_with_progress(self, job_id: str, database: str | None = None) -> TaskWithProgress:
        """Return the task with progress for the given job_id."""
        pass
