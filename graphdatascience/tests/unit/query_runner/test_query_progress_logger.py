import time
from typing import Optional, Dict, Any

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.query_runner.query_progress_logger import QueryProgressLogger


def test_call_through_functions() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:

        assert query == "CALL gds.listProgress('foo') YIELD taskName, progress RETURN taskName, progress LIMIT 1"
        assert database == "database"

        return DataFrame([{"progress": "n/a", "taskName": "Test task"}])

    def fake_query() -> DataFrame:
        time.sleep(1)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(3,0,0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_skips_progress_logging_for_old_server_version() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:  # type: ignore
        print("Should not be called!")
        assert False

    def fake_query() -> DataFrame:
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2,0,0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42


def test_uses_beta_endpoint() -> None:
    def fake_run_cypher(query: str, database: Optional[str] = None) -> DataFrame:

        assert query == "CALL gds.beta.listProgress('foo') YIELD taskName, progress RETURN taskName, progress LIMIT 1"
        assert database == "database"

        return DataFrame([{"progress": "n/a", "taskName": "Test task"}])

    def fake_query() -> DataFrame:
        time.sleep(1)
        return DataFrame([{"result": 42}])

    qpl = QueryProgressLogger(fake_run_cypher, lambda: ServerVersion(2,4,0))
    df = qpl.run_with_progress_logging(fake_query, "foo", "database")

    assert df["result"][0] == 42
