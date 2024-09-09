import pytest

from graphdatascience.query_runner.progress.progress_provider import TaskWithProgress
from graphdatascience.query_runner.progress.static_progress_provider import StaticProgressStore


@pytest.fixture(autouse=True)
def clear_progress_store():
    StaticProgressStore._progress_store = {}
    yield


def test_task_registration():
    StaticProgressStore.register_task_with_unknown_volume("test-job", "Test task")
    assert StaticProgressStore._progress_store == {"test-job": TaskWithProgress("Test task", "n/a")}


def test_returns_task_by_job_id():
    StaticProgressStore._progress_store = {"test-job": TaskWithProgress("Test task", "n/a")}
    task = StaticProgressStore.get_task_with_volume("test-job")
    assert task.task_name == "Test task"
    assert task.progress_percent == "n/a"


def test_contains_job_id():
    StaticProgressStore._progress_store = {"test-job": TaskWithProgress("Test task", "n/a")}
    assert StaticProgressStore.contains_job_id("test-job")
    assert not StaticProgressStore.contains_job_id("unknown-job")
