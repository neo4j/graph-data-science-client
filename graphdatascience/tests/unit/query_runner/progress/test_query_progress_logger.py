import time
from typing import Optional

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.query_runner.progress.query_progress_logger import QueryProgressLogger
from graphdatascience.query_runner.progress.query_progress_provider import QueryProgressProvider
from graphdatascience.query_runner.progress.static_progress_provider import StaticProgressProvider, StaticProgressStore
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_call_through_functions() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
        assert "CALL gds.listProgress('foo')" in query
        assert database == "database"

        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    def fake_query() -> DataFrame:
        time.sleep(1)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(3, 0, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_skips_progress_logging_for_old_server_version() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
        print("Should not be called!")
        assert False

    def fake_query() -> DataFrame:
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2, 0, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_uses_beta_endpoint() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
        assert "CALL gds.beta.listProgress('foo')" in query
        assert database == "database"

        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    def fake_query() -> DataFrame:
        time.sleep(1)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2, 4, 0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_uses_query_provider() -> None:
    server_version = ServerVersion(3, 0, 0)
    query_runner = CollectingQueryRunner(server_version)

    def simple_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
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

    def simple_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
        return query_runner.run_cypher(query, db=database)

    qpl = QueryProgressLogger(simple_run_cypher, lambda: server_version)
    progress_provider = qpl._select_progress_provider("test-job")
    assert isinstance(progress_provider, QueryProgressProvider)

    progress = progress_provider.root_task_with_progress("test-job", "database")

    assert progress.sub_tasks_description == "root 1/1::leaf"
    assert progress.task_name == "Test task"


def test_uses_static_store() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:
        return DataFrame([{"progress": "n/a", "taskName": "Test task", "status": "RUNNING"}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(3, 0, 0))
    StaticProgressStore.register_task_with_unknown_volume("test-job", "Test task")

    progress_provider = qpl._select_progress_provider("test-job")
    assert isinstance(progress_provider, StaticProgressProvider)
    task_with_volume = progress_provider.root_task_with_progress("test-job")
    assert task_with_volume.task_name == "Test task"
    assert task_with_volume.progress_percent == "n/a"
    assert task_with_volume.sub_tasks_description is None
