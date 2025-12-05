from __future__ import annotations

from types import TracebackType
from typing import Any, Type

from tqdm.auto import tqdm

from graphdatascience.query_runner.progress.progress_provider import TaskWithProgress


class TqdmProgressBar:
    def __init__(self, task_name: str, relative_progress: float | None, bar_options: dict[str, Any] = {}):
        root_task_name = task_name
        if relative_progress is None:  # Qualitative progress report
            self._tqdm_bar = tqdm(
                total=None,
                unit="",
                desc=root_task_name,
                bar_format="{desc} [elapsed: {elapsed} {postfix}]",
                **bar_options,
            )
        else:
            self._tqdm_bar = tqdm(
                total=100,
                unit="%",
                desc=root_task_name,
                initial=relative_progress,
                **bar_options,
            )

    def __enter__(self: TqdmProgressBar) -> TqdmProgressBar:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.finish(success=exception_value is None)

    def update(
        self,
        status: str,
        progress: float | None,
        sub_tasks_description: str | None = None,
    ) -> None:
        postfix = f"status: {status}, task: {sub_tasks_description}" if sub_tasks_description else f"status: {status}"
        self._tqdm_bar.set_postfix_str(postfix, refresh=False)
        if progress is not None:
            new_progress = progress - self._tqdm_bar.n
            self._tqdm_bar.update(new_progress)
        else:
            self._tqdm_bar.refresh()

    def finish(self, success: bool) -> None:
        if not success:
            self._tqdm_bar.set_postfix_str("status: FAILED", refresh=True)
        else:
            if self._tqdm_bar.total is not None:
                self._tqdm_bar.update(self._tqdm_bar.total - self._tqdm_bar.n)
            self._tqdm_bar.set_postfix_str("status: FINISHED", refresh=True)
        self._tqdm_bar.close()

    @staticmethod
    def _relative_progress(task: TaskWithProgress) -> float | None:
        try:
            return float(task.progress_percent.removesuffix("%"))
        except ValueError:
            return None
