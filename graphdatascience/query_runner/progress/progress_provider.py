from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class TaskWithProgress:
    task_name: str
    progress_percent: str
    status: str
    sub_tasks_description: Optional[str] = None


class ProgressProvider(ABC):
    @abstractmethod
    def root_task_with_progress(self, job_id: str, database: Optional[str] = None) -> TaskWithProgress:
        """Return the task with progress for the given job_id."""
        pass
