from typing import Any, Generator

import pytest

from graphdatascience.query_runner.progress.progress_provider import TaskWithProgress
from graphdatascience.query_runner.progress.static_progress_provider import StaticProgressStore


@pytest.fixture(autouse=True)
def clear_progress_store() -> Generator[None, Any, None]:
    StaticProgressStore._progress_store = {}
    yield


def test_task_registration() -> None:
    StaticProgressStore.register_task_with_unknown_volume("test-job", "Test task")
    assert StaticProgressStore._progress_store == {"test-job": TaskWithProgress("Test task", "n/a", "RUNNING")}


def test_returns_task_by_job_id() -> None:
    task = TaskWithProgress("Test task", "n/a", "RUNNING")
    StaticProgressStore._progress_store = {"test-job": TaskWithProgress("Test task", "n/a", "RUNNING")}
    actualTask = StaticProgressStore.get_task_with_volume("test-job")
    assert task == actualTask


def test_contains_job_id() -> None:
    StaticProgressStore._progress_store = {"test-job": TaskWithProgress("Test task", "n/a", "RUNNING")}
    assert StaticProgressStore.contains_job_id("test-job")
    assert not StaticProgressStore.contains_job_id("unknown-job")
