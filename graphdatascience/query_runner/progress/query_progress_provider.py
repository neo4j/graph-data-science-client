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
        # we only retrieve the progress of the root task
        progress = self._run_cypher_func(
            f"CALL gds.{tier}listProgress('{job_id}')"
            + " YIELD taskName, progress"
            + " RETURN taskName, progress"
            + " LIMIT 1",
            database,
        )

        progress_percent = progress["progress"][0]
        root_task_name = progress["taskName"][0].split("|--")[-1][1:]

        return TaskWithProgress(root_task_name, progress_percent)
