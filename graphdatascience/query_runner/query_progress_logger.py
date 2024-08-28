import warnings
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, NoReturn, Optional
from uuid import uuid4

from pandas import DataFrame
from tqdm.auto import tqdm

from ..server_version.server_version import ServerVersion

# takes a query str, optional db str and returns the result as a DataFrame
CypherQueryFunction = Callable[[str, Optional[str]], DataFrame]
DataFrameProducer = Callable[[], DataFrame]


class QueryProgressLogger:
    _LOG_POLLING_INTERVAL = 0.5

    def __init__(
        self,
        run_cypher_func: CypherQueryFunction,
        server_version: ServerVersion,
    ):
        self.run_cypher_func = run_cypher_func
        self._server_version = server_version

    @staticmethod
    def extract_or_create_job_id(params: Dict[str, Any]) -> str:
        if "config" in params:
            if "jobId" in params["config"]:
                job_id = params["config"]["jobId"]
            else:
                job_id = str(uuid4())
                params["config"]["jobId"] = job_id
        else:
            job_id = str(uuid4())
            params["config"] = {"jobId": job_id}

        return job_id

    def run_with_progress_logging(
        self, runnable: DataFrameProducer, job_id: str, database: Optional[str] = None
    ) -> DataFrame:
        if self._server_version < ServerVersion(2, 1, 0):
            return runnable()

        with ThreadPoolExecutor() as executor:
            future = executor.submit(runnable)

            self._log(future, job_id, database)

            if future.exception():
                raise future.exception()  # type: ignore
            else:
                return future.result()

    def _log(self, future: "Future[Any]", job_id: str, database: Optional[str] = None) -> None:
        pbar: Optional[tqdm[NoReturn]] = None
        warn_if_failure = True

        while wait([future], timeout=self._LOG_POLLING_INTERVAL).not_done:
            try:
                tier = "beta." if self._server_version < ServerVersion(2, 5, 0) else ""
                # we only retrieve the progress of the root task
                progress = self.run_cypher_func(
                    f"CALL gds.{tier}listProgress('{job_id}')"
                    + " YIELD taskName, progress"
                    + " RETURN taskName, progress"
                    + " LIMIT 1",
                    database,
                )
            except Exception as e:
                # Do nothing if the procedure either:
                # * has not started yet,
                # * has already completed.
                if f"No task with job id `{job_id}` was found" in str(e):
                    continue
                else:
                    if warn_if_failure:
                        warnings.warn(f"Unable to get progress: {str(e)}", RuntimeWarning)
                        warn_if_failure = False
                    continue

            progress_percent = progress["progress"][0]
            if progress_percent == "n/a":
                return

            root_task_name = progress["taskName"][0].split("|--")[-1][1:]
            if not pbar:
                pbar = tqdm(total=100, unit="%", desc=root_task_name, maxinterval=self._LOG_POLLING_INTERVAL)

            parsed_progress = float(progress_percent[:-1])
            pbar.update(parsed_progress - pbar.n)

        if pbar:
            pbar.update(100 - pbar.n)
            pbar.refresh()
