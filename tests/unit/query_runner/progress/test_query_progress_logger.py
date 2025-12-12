import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from io import StringIO

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.query_runner.progress.progress_provider import TaskWithProgress
from graphdatascience.query_runner.progress.query_progress_logger import QueryProgressLogger
from graphdatascience.query_runner.progress.query_progress_provider import QueryProgressProvider
from graphdatascience.query_runner.progress.static_progress_provider import StaticProgressProvider, StaticProgressStore
from tests.unit.conftest import CollectingQueryRunner


def test_call_through_functions() -> None:
    progress_fetched_event = threading.Event()
    progress_called = []

    def fake_run_cypher(query: str, database: str | None = None) -> DataFrame:
        progress_called.append(time.time())

        assert "CALL gds.listProgress('foo')" in query
        assert database == "database"

        progress_fetched_event.set()

        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    def fake_query() -> DataFrame:
        progress_fetched_event.wait(5)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(3, 0, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert len(progress_called) > 0
    assert df["result"][0] == 42


def test_skips_progress_logging_for_old_server_version() -> None:
    def fake_run_cypher(query: str, database: str | None = None) -> DataFrame:
        print("Should not be called!")
        assert False

    def fake_query() -> DataFrame:
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2, 0, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_uses_beta_endpoint() -> None:
    progress_fetched_event = threading.Event()

    def fake_run_cypher(query: str, database: str | None = None) -> DataFrame:
        assert "CALL gds.beta.listProgress('foo')" in query
        assert database == "database"

        progress_fetched_event.set()

        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    def fake_query() -> DataFrame:
        progress_fetched_event.wait(5)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2, 4, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_uses_query_provider() -> None:
    server_version = ServerVersion(3, 0, 0)
    query_runner = CollectingQueryRunner(server_version)

    def simple_run_cypher(query: str, database: str | None = None) -> DataFrame:
        return query_runner.run_cypher(query, db=database)

    qpl = QueryProgressLogger(simple_run_cypher, lambda: server_version)
    progress_provider = qpl._select_progress_provider("test-job")
    assert isinstance(progress_provider, QueryProgressProvider)


def test_uses_query_provider_with_task_description() -> None:
    server_version = ServerVersion(3, 0, 0)
    detailed_progress = DataFrame(
        [
            {"progress": "n/a", "taskName": "Test task", "status": "RUNNING"},
            {"progress": "n/a", "taskName": "    |-- root 1/1", "status": "RUNNING"},
            {"progress": "n/a", "taskName": "       |-- leaf", "status": "RUNNING"},
            {"progress": "n/a", "taskName": "finished task", "status": "FINISHED"},
            {"progress": "n/a", "taskName": "pending task", "status": "PENDING"},
        ]
    )

    query_runner = CollectingQueryRunner(server_version, result_mock={"gds.listProgress": detailed_progress})

    def simple_run_cypher(query: str, database: str | None = None) -> DataFrame:
        return query_runner.run_cypher(query, db=database)

    qpl = QueryProgressLogger(simple_run_cypher, lambda: server_version)
    progress_provider = qpl._select_progress_provider("test-job")
    assert isinstance(progress_provider, QueryProgressProvider)

    progress = progress_provider.root_task_with_progress("test-job", "database")

    assert progress.sub_tasks_description == "root 1/1::leaf"
    assert progress.task_name == "Test task"


def test_progress_bar_quantitive_output() -> None:
    def simple_run_cypher(query: str, database: str | None = None) -> DataFrame:
        raise NotImplementedError("Should not be called!")

    with StringIO() as pbarOutputStream:
        qpl = QueryProgressLogger(
            simple_run_cypher,
            lambda: ServerVersion(3, 0, 0),
            progress_bar_options={"file": pbarOutputStream, "mininterval": 0},
        )

        pbar = qpl._init_pbar(TaskWithProgress("test task", "0%", "PENDING", ""))
        assert pbarOutputStream.getvalue().split("\r")[-1] == "test task:   0%|          | 0.0/100 [00:00<?, ?%/s]"

        qpl._update_pbar(pbar, TaskWithProgress("test task", "0%", "PENDING", ""))
        assert (
            pbarOutputStream.getvalue().split("\r")[-1]
            == "test task:   0%|          | 0.0/100 [00:00<?, ?%/s, status: PENDING]"
        )
        qpl._update_pbar(pbar, TaskWithProgress("test task", "42%", "RUNNING", "root::1/1::leaf"))

        running_output = pbarOutputStream.getvalue().split("\r")[-1]
        assert re.match(
            r"test task:  42%\|####2     \| 42.0/100 \[00:00<00:00, \d+.\d*%/s, status: RUNNING, task: root::1/1::leaf\]",
            running_output,
        ), running_output

        with ThreadPoolExecutor() as executor:
            future = executor.submit(lambda: None)
            qpl._finish_pbar(future, pbar)

        finished_output = pbarOutputStream.getvalue().split("\r")[-1]
        assert re.match(
            r"test task: 100%\|##########\| 100.0/100 \[00:00<00:00, \d+.\d+%/s, status: FINISHED\]", finished_output
        ), finished_output


def test_progress_bar_qualitative_output() -> None:
    def simple_run_cypher(query: str, database: str | None = None) -> DataFrame:
        raise NotImplementedError("Should not be called!")

    with StringIO() as pbarOutputStream:
        qpl = QueryProgressLogger(
            simple_run_cypher,
            lambda: ServerVersion(3, 0, 0),
            progress_bar_options={"file": pbarOutputStream, "mininterval": 100},
        )

        pbar = qpl._init_pbar(TaskWithProgress("test task", "n/a", "PENDING", ""))
        assert re.match(
            r"test task \[elapsed: 00:00 \]",
            last_output_line(pbarOutputStream),
        ), last_output_line(pbarOutputStream)

        qpl._update_pbar(pbar, TaskWithProgress("test task", "n/a", "PENDING", ""))
        assert re.match(
            r"test task \[elapsed: 00:00 , status: PENDING\]",
            last_output_line(pbarOutputStream),
        ), last_output_line(pbarOutputStream)

        qpl._update_pbar(pbar, TaskWithProgress("test task", "", "RUNNING", "root 1/1::leaf"))
        assert re.match(
            r"test task \[elapsed: 00:00 , status: RUNNING, task: root 1/1::leaf\]",
            last_output_line(pbarOutputStream),
        ), last_output_line(pbarOutputStream)

        with ThreadPoolExecutor() as executor:
            future = executor.submit(lambda: None)
            qpl._finish_pbar(future, pbar)

            assert re.match(
                r"test task \[elapsed: 00:00 , status: FINISHED\]",
                last_output_line(pbarOutputStream),
            ), last_output_line(pbarOutputStream)


def test_progress_bar_with_failing_query() -> None:
    def simple_run_cypher(query: str, database: str | None = None) -> DataFrame:
        raise NotImplementedError("Should not be called!")

    def failing_runnable() -> DataFrame:
        raise NotImplementedError("Should not be called!")

    with StringIO() as pbarOutputStream:
        qpl = QueryProgressLogger(
            simple_run_cypher,
            lambda: ServerVersion(3, 0, 0),
            progress_bar_options={"file": pbarOutputStream, "mininterval": 100},
        )

        with ThreadPoolExecutor() as executor:
            future = executor.submit(failing_runnable)

            pbar = qpl._init_pbar(TaskWithProgress("test task", "n/a", "PENDING", ""))
            assert re.match(
                r"test task \[elapsed: 00:00 \]",
                last_output_line(pbarOutputStream),
            ), last_output_line(pbarOutputStream)

            qpl._finish_pbar(future, pbar)

            assert re.match(
                r"test task \[elapsed: \d{2}:\d{2} , status: FAILED\]", last_output_line(pbarOutputStream)
            ), last_output_line(pbarOutputStream)


def test_uses_static_store() -> None:
    def fake_run_cypher(query: str, database: str | None = None) -> DataFrame:
        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(3, 0, 0))
    StaticProgressStore.register_task_with_unknown_volume("test-job", "Test task")

    progress_provider = qpl._select_progress_provider("test-job")
    assert isinstance(progress_provider, StaticProgressProvider)
    task_with_volume = progress_provider.root_task_with_progress("test-job")
    assert task_with_volume.task_name == "Test task"
    assert task_with_volume.progress_percent == "n/a"
    assert task_with_volume.sub_tasks_description is None


def last_output_line(output_stream: StringIO) -> str:
    return output_stream.getvalue().split("\r")[-1]
