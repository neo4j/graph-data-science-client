from typing import Any, Protocol

from pandas import DataFrame

from .progress_provider import ProgressProvider, TaskWithProgress


class CypherQueryFunction(Protocol):
    def __call__(
        self,
        query: str,
        database: str | None,
        params: dict[str, Any] | None = None,
    ) -> DataFrame: ...


class QueryProgressProvider(ProgressProvider):
    def __init__(self, run_cypher_func: CypherQueryFunction):
        self._run_cypher_func = run_cypher_func

    def root_task_with_progress(self, job_id: str, database: str | None = None) -> TaskWithProgress:
        # expect at exactly one row (query will fail if not existing)
        progress = self._run_cypher_func(
            "CALL gds.listProgress($job_id)"
            + " YIELD taskName, progress, status"
            + " RETURN taskName, progress, status",
            database=database,
            params={"job_id": job_id},
        )

        # compute depth of each subtask
        progress["trimmedName"] = progress["taskName"].str.lstrip()
        progress["depth"] = progress["taskName"].str.len() - progress["trimmedName"].str.len()
        progress.sort_values("depth", ascending=True, inplace=True)

        root_task = progress.iloc[0]
        root_progress_percent = root_task["progress"]
        root_task_name = root_task["trimmedName"].replace("|--", "")
        root_status = root_task["status"]

        subtask_descriptions = None
        running_tasks = progress[progress["status"] == "RUNNING"]
        if running_tasks["taskName"].size > 1:  # at least one subtask
            subtasks = running_tasks[1:]  # remove root task
            subtask_descriptions = "::".join(
                list(subtasks["taskName"].apply(lambda name: name.split("|--")[-1].strip()))
            )

        return TaskWithProgress(
            root_task_name, root_progress_percent, root_status, sub_tasks_description=subtask_descriptions
        )
