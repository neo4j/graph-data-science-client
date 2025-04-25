import warnings
from concurrent import futures
from typing import Any, Callable, NoReturn, Optional, Union

from pandas import DataFrame
from tenacity import Retrying, wait
from tqdm.auto import tqdm

from graphdatascience.retry_utils.retry_utils import retry_until_future

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
        log_interval: Union[float, wait.wait_base] = 0.5,
        initial_wait_time: float = 0.5,
        progress_bar_options: dict[str, Any] = {},
    ):
        self._run_cypher_func = run_cypher_func
        self._server_version_func = server_version_func
        self._static_progress_provider = StaticProgressProvider()
        self._query_progress_provider = QueryProgressProvider(run_cypher_func, server_version_func)
        self._progress_bar_options = progress_bar_options

        self._initial_wait_time = initial_wait_time
        if isinstance(log_interval, float):
            self._wait_base: wait.wait_base = wait.wait_fixed(log_interval)
        elif isinstance(log_interval, wait.wait_base):
            self._wait_base = log_interval
        else:
            raise ValueError("polling interval must be a float or an instance of wait_base")

    def run_with_progress_logging(
        self, runnable: DataFrameProducer, job_id: str, database: Optional[str] = None
    ) -> DataFrame:
        if self._server_version_func() < ServerVersion(2, 1, 0):
            return runnable()

        # Select progress provider based on whether the job id is in the static progress store.
        # Entries in the static progress store are already visible at this point.
        progress_provider = self._select_progress_provider(job_id)

        with futures.ThreadPoolExecutor() as executor:
            future = executor.submit(runnable)

            futures.wait([future], timeout=self._initial_wait_time)  # wait for progress task to be available
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
        self,
        future: futures.Future[Any],
        job_id: str,
        progress_provider: ProgressProvider,
        database: Optional[str] = None,
    ) -> None:
        pbar: Optional[tqdm[NoReturn]] = None
        warn_if_failure = True

        for attempt in Retrying(wait=self._wait_base, retry=retry_until_future(future)):
            with attempt:
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

                    if warn_if_failure:
                        warnings.warn(f"Unable to get progress: {str(e)}", RuntimeWarning)
                        warn_if_failure = False

        if pbar is not None:
            self._finish_pbar(future, pbar)

    def _init_pbar(self, task_with_progress: TaskWithProgress) -> tqdm:  # type: ignore
        root_task_name = task_with_progress.task_name
        parsed_progress = QueryProgressLogger._relative_progress(task_with_progress)
        if parsed_progress is None:  # Qualitative progress report
            return tqdm(
                total=None,
                unit="",
                desc=root_task_name,
                bar_format="{desc} [elapsed: {elapsed} {postfix}]",
                **self._progress_bar_options,
            )
        else:
            return tqdm(
                total=100,
                unit="%",
                desc=root_task_name,
                **self._progress_bar_options,
            )

    def _update_pbar(self, pbar: tqdm, task_with_progress: TaskWithProgress) -> None:  # type: ignore
        parsed_progress = QueryProgressLogger._relative_progress(task_with_progress)
        postfix = (
            f"status: {task_with_progress.status}, task: {task_with_progress.sub_tasks_description}"
            if task_with_progress.sub_tasks_description
            else f"status: {task_with_progress.status}"
        )
        pbar.set_postfix_str(postfix, refresh=False)
        if parsed_progress is not None:
            new_progress = parsed_progress - pbar.n
            pbar.update(new_progress)
        else:
            pbar.refresh()

    def _finish_pbar(self, future: futures.Future[Any], pbar: tqdm) -> None:  # type: ignore
        if future.exception():
            pbar.set_postfix_str("status: FAILED", refresh=True)
            return

        if pbar.total is not None:
            pbar.update(pbar.total - pbar.n)
        pbar.set_postfix_str("status: FINISHED", refresh=True)

    @staticmethod
    def _relative_progress(task: TaskWithProgress) -> Optional[float]:
        try:
            return float(task.progress_percent.removesuffix("%"))
        except ValueError:
            return None
