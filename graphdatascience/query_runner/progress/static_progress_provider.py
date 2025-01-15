from typing import Optional

from .progress_provider import ProgressProvider, TaskWithProgress


class StaticProgressStore:
    _progress_store: dict[str, TaskWithProgress] = {}

    @staticmethod
    def register_task_with_unknown_volume(job_id: str, task_name: str) -> None:
        StaticProgressStore._progress_store[job_id] = TaskWithProgress(task_name, "n/a", "RUNNING")

    @staticmethod
    def get_task_with_volume(job_id: str) -> TaskWithProgress:
        return StaticProgressStore._progress_store[job_id]

    @staticmethod
    def contains_job_id(job_id: str) -> bool:
        return job_id in StaticProgressStore._progress_store


class StaticProgressProvider(ProgressProvider):
    def root_task_with_progress(self, job_id: str, database: Optional[str] = None) -> TaskWithProgress:
        if not StaticProgressStore.contains_job_id(job_id):
            raise Exception(f"Task with job id {job_id} not found in progress store")

        return StaticProgressStore.get_task_with_volume(job_id)
