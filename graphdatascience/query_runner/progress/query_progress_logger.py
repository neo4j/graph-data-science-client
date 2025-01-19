import warnings
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable, NoReturn, Optional

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

    def _select_progress_provider(self, job_id: str) -> ProgressProvider:
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

                has_relative_progress = progress_percent != "n/a"
                if pbar is None:
                    if has_relative_progress:
                        pbar = tqdm(total=100, unit="%", desc=root_task_name, maxinterval=self._LOG_POLLING_INTERVAL)
                    else:
                        pbar = tqdm(
                            total=None,
                            unit="",
                            desc=root_task_name,
                            maxinterval=self._LOG_POLLING_INTERVAL,
                            bar_format="{desc} [elapsed: {elapsed} {postfix}]",
                        )

                pbar.set_postfix_str(
                    f"status: {task_with_progress.status}, task: {task_with_progress.sub_tasks_description}"
                )
                if has_relative_progress:
                    parsed_progress = float(progress_percent[:-1])
                    new_progress = parsed_progress - pbar.n
                    pbar.update(new_progress)
                else:
                    pbar.refresh()  # show latest elapsed time + postfix
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

        if pbar is not None:
            if pbar.total is not None:
                pbar.update(pbar.total - pbar.n)
            pbar.set_postfix_str("status: finished")
            pbar.refresh()
