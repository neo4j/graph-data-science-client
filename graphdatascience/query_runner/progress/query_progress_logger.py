import warnings
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import Any, Callable, Optional

from pandas import DataFrame

from graphdatascience.query_runner.progress.progress_bar import TqdmProgressBar

from ...server_version.server_version import ServerVersion
from .progress_provider import ProgressProvider, TaskWithProgress
from .query_progress_provider import CypherQueryFunction, QueryProgressProvider, ServerVersionFunction
from .static_progress_provider import StaticProgressProvider, StaticProgressStore

DataFrameProducer = Callable[[], DataFrame]


class QueryProgressLogger:
    def __init__(
        self,
        run_cypher_func: CypherQueryFunction,
        server_version_func: ServerVersionFunction,
        polling_interval: float = 0.5,
        progress_bar_options: dict[str, Any] = {},
    ):
        self._run_cypher_func = run_cypher_func
        self._server_version_func = server_version_func
        self._static_progress_provider = StaticProgressProvider()
        self._query_progress_provider = QueryProgressProvider(run_cypher_func, server_version_func)
        self._polling_interval = polling_interval
        self._progress_bar_options = progress_bar_options

        self._progress_bar_options.setdefault("maxinterval", self._polling_interval)

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
        self, future: Future[Any], job_id: str, progress_provider: ProgressProvider, database: Optional[str] = None
    ) -> None:
        pbar: Optional[TqdmProgressBar] = None
        warn_if_failure = True

        while wait([future], timeout=self._polling_interval).not_done:
            try:
                task_with_progress = progress_provider.root_task_with_progress(job_id, database)
                if pbar is None:
                    pbar = self._init_pbar(task_with_progress)

                self._update_pbar(pbar, task_with_progress)
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
            self._finish_pbar(future, pbar)

    def _update_pbar(self, pbar: TqdmProgressBar, task: TaskWithProgress) -> None:
        pbar.update(
            task.status,
            task.relative_progress(),
            task.sub_tasks_description,
        )

    def _init_pbar(self, task: TaskWithProgress) -> TqdmProgressBar:
        return TqdmProgressBar(
            task.task_name,
            task.relative_progress(),
            bar_options=self._progress_bar_options,
        )

    def _finish_pbar(self, future: Future[Any], pbar: TqdmProgressBar) -> None:
        pbar.finish(future.exception() is None)
