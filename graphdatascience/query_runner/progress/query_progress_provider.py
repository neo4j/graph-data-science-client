from typing import Callable, Optional

from pandas import DataFrame

from ...server_version.server_version import ServerVersion
from .progress_provider import ProgressProvider, TaskWithProgress

# takes a query str, optional db str and returns the result as a DataFrame
CypherQueryFunction = Callable[[str, Optional[str]], DataFrame]
ServerVersionFunction = Callable[[], ServerVersion]


class QueryProgressProvider(ProgressProvider):
    def __init__(
        self,
        run_cypher_func: CypherQueryFunction,
        server_version_func: ServerVersionFunction,
    ):
        self._run_cypher_func = run_cypher_func
        self._server_version_func = server_version_func

    def root_task_with_progress(self, job_id: str, database: Optional[str] = None) -> TaskWithProgress:
        tier = "beta." if self._server_version_func() < ServerVersion(2, 5, 0) else ""

        # expect at exactly one row (query will fail if not existing)
        progress = self._run_cypher_func(
            f"CALL gds.{tier}listProgress('{job_id}')"
            + " YIELD taskName, progress, status"
            + " RETURN taskName, progress, status",
            database,
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
