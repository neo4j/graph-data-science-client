from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Type

from tqdm.auto import tqdm

from graphdatascience.progress.progress_provider import TaskWithProgress


class ProgressBar(ABC):
    @abstractmethod
    def __enter__(self) -> ProgressBar:
        pass

    @abstractmethod
    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    @abstractmethod
    def update(
        self,
        status: str,
        progress: float | None,
        sub_tasks_description: str | None = None,
        step: str | None = None,
    ) -> None:
        pass

    @abstractmethod
    def finish(self, success: bool, step: str | None = None, sub_tasks_description: str | None = None) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        """Abandon the bar without leaving a final line, e.g. to replace it with a new one."""
        pass


class TqdmProgressBar(ProgressBar):
    # Process-wide tqdm options merged into every bar's own ``bar_options``
    # (per-call options win on conflict). Lets a caller like the CLI set e.g.
    # ``{"leave": False}`` once so all session progress bars clear themselves
    # when done instead of piling up in the output.
    _default_options: dict[str, Any] = {}

    @classmethod
    def set_default_options(cls, options: dict[str, Any]) -> None:
        cls._default_options = dict(options)

    def __init__(self, task_name: str, relative_progress: float | None, bar_options: dict[str, Any] | None = None):
        root_task_name = task_name
        options = {**self._default_options, **(bar_options or {})}
        if relative_progress is not None:
            # Keep the displayed fraction readable instead of e.g. 55.55555555555556/100.
            relative_progress = round(relative_progress, 4)
        if relative_progress is None:  # Qualitative progress report
            self._tqdm_bar = tqdm(
                total=None,
                unit="",
                desc=root_task_name,
                bar_format="{desc} [elapsed: {elapsed} {postfix}]",
                **options,
            )
        else:
            self._tqdm_bar = tqdm(
                total=100,
                unit="%",
                desc=root_task_name,
                initial=relative_progress,
                **options,
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

    @staticmethod
    def _postfix(status: str, step: str | None, sub_tasks_description: str | None) -> str:
        postfix_parts = [f"status: {status}"]
        if step is not None:
            postfix_parts.append(f"step: {step}")
        if sub_tasks_description:
            postfix_parts.append(f"task: {sub_tasks_description}")
        return ", ".join(postfix_parts)

    def update(
        self,
        status: str,
        progress: float | None,
        sub_tasks_description: str | None = None,
        step: str | None = None,
    ) -> None:
        self._tqdm_bar.set_postfix_str(self._postfix(status, step, sub_tasks_description), refresh=False)

        if progress is not None:
            # tqdm skips the redraw when the delta is below its dynamic threshold, which
            # would leave a changed postfix invisible, so refresh explicitly in that case.
            if not self._tqdm_bar.update(round(progress, 4) - self._tqdm_bar.n):
                self._tqdm_bar.refresh()
        else:
            self._tqdm_bar.refresh()

    def close(self) -> None:
        self._tqdm_bar.leave = False
        self._tqdm_bar.close()

    def finish(self, success: bool, step: str | None = None, sub_tasks_description: str | None = None) -> None:
        if success and self._tqdm_bar.total is not None:
            self._tqdm_bar.update(self._tqdm_bar.total - self._tqdm_bar.n)
        status = "FINISHED" if success else "FAILED"
        self._tqdm_bar.set_postfix_str(self._postfix(status, step, sub_tasks_description), refresh=True)
        self._tqdm_bar.close()

    @staticmethod
    def _relative_progress(task: TaskWithProgress) -> float | None:
        try:
            return float(task.progress_percent.removesuffix("%"))
        except ValueError:
            return None


class NoOpProgressBar(ProgressBar):
    def __enter__(self: NoOpProgressBar) -> NoOpProgressBar:
        return self

    def __exit__(
        self,
        exception_type: Type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    def update(
        self,
        status: str,
        progress: float | None,
        sub_tasks_description: str | None = None,
        step: str | None = None,
    ) -> None:
        pass

    def finish(self, success: bool, step: str | None = None, sub_tasks_description: str | None = None) -> None:
        pass

    def close(self) -> None:
        pass
