import warnings
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable, Dict, NoReturn, Optional
from uuid import uuid4

from pandas import DataFrame
from tqdm.auto import tqdm

from ...server_version.server_version import ServerVersion
from .progress_provider import ProgressProvider
from .query_progress_provider import CypherQueryFunction, QueryProgressProvider, ServerVersionFunction
from .static_progress_provider import StaticProgressProvider, StaticProgressStore

DataFrameProducer = Callable[[], DataFrame]


class QueryProgressLogger:
    _LOG_POLLING_INTERVAL = 0.5

    def __init__(
        self,
        run_cypher_func: CypherQueryFunction,
        server_version_func: ServerVersionFunction,
    ):
        self._run_cypher_func = run_cypher_func
        self._server_version_func = server_version_func
        self._static_progress_provider = StaticProgressProvider()
        self._query_progress_provider = QueryProgressProvider(run_cypher_func, server_version_func)

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
        if self._server_version_func() < ServerVersion(2, 1, 0):
            return runnable()

        # Select progress provider based on whether the job id is in the static progress store.
        # Entries in the static progress store are already visible at this point.
        progress_provider = self._select_progress_provider(job_id)

        with ThreadPoolExecutor() as executor:
            future = executor.submit(runnable)

            self._log(future, job_id, progress_provider, database)

            if future.exception():
                raise future.exception()  # type: ignore
            else:
                return future.result()

    def _select_progress_provider(self, job_id) -> ProgressProvider:
        return (
            self._static_progress_provider
            if StaticProgressStore.contains_job_id(job_id)
            else self._query_progress_provider
        )

    def _log(
        self, future: "Future[Any]", job_id: str, progress_provider: ProgressProvider, database: Optional[str] = None
    ) -> None:
        pbar: Optional[tqdm[NoReturn]] = None
        warn_if_failure = True

        while wait([future], timeout=self._LOG_POLLING_INTERVAL).not_done:
            try:
                task_with_progress = progress_provider.root_task_with_progress(job_id, database)
                root_task_name = task_with_progress.task_name
                progress_percent = task_with_progress.progress_percent

                if progress_percent == "n/a":
                    return

                if not pbar:
                    pbar = tqdm(total=100, unit="%", desc=root_task_name, maxinterval=self._LOG_POLLING_INTERVAL)

                parsed_progress = float(progress_percent[:-1])
                pbar.update(parsed_progress - pbar.n)
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

        if pbar:
            pbar.update(100 - pbar.n)
            pbar.refresh()
