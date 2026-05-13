from unittest import mock

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import ConnectionInfo
from graphdatascience.arrow_client.v1.gds_arrow_client import GdsArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.session_query_runner import SessionQueryRunner
from tests.unit.conftest import CollectingQueryRunner


class FakeArrowClient(GdsArrowClient):
    def __init__(self) -> None:
        super().__init__(flight_client=mock.Mock(spec=GdsArrowClient))

    def advertised_connection_info(self) -> ConnectionInfo:
        return ConnectionInfo("myHost", 1234, encrypted=False)

    def request_token(self) -> str:
        return "myToken"


def test_extracts_parameters_algo_write_v1() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}]))
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(gds_query_runner, db_query_runner, FakeArrowClient(), True)  # type: ignore

    qr.call_procedure(endpoint="gds.degree.write", params=CallParameters(graph_name="g", config={"jobId": "my-job"}))

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"jobId": "my-job", "writeToResultStore": True},
    }
    assert (
        db_query_runner.last_query() == "CALL gds.arrow.write($graphName, $databaseName, $jobId, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "jobId": "my-job",
        "arrowConfiguration": {"encrypted": False, "host": "myHost", "port": "1234", "token": "myToken"},
    }


def test_extracts_parameters_algo_write_v2() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}, {"version": "v2"}]))
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(
        gds_query_runner,
        db_query_runner,
        FakeArrowClient(),  # type: ignore
        True,
    )

    qr.call_procedure(
        endpoint="gds.degree.write", params=CallParameters(graph_name="g", config={"jobId": "my-job", "concurrency": 2})
    )

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"jobId": "my-job", "writeToResultStore": True, "concurrency": 2},
    }
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.write.v2($graphName, $jobId, $arrowConfiguration, $configuration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "jobId": "my-job",
        "arrowConfiguration": {"encrypted": False, "host": "myHost", "port": "1234", "token": "myToken"},
        "configuration": {"concurrency": 2},
    }


def test_extracts_parameters_algo_write_estimate() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}, {"version": "v2"}]))
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(
        gds_query_runner,
        db_query_runner,
        FakeArrowClient(),  # type: ignore
        True,
    )

    qr.call_procedure(
        endpoint="gds.degree.write.estimate",
        params=CallParameters(graph_name="g", config={"jobId": "my-job", "concurrency": 2}),
    )

    assert gds_query_runner.last_query() == "CALL gds.degree.write.estimate($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"jobId": "my-job", "concurrency": 2},
    }
    assert db_query_runner.last_query() == "CALL gds.session.dbms.protocol.version() YIELD version"
    assert db_query_runner.last_params() == {}


def test_arrow_and_write_configuration() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}]))
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(gds_query_runner, db_query_runner, FakeArrowClient(), True)  # type: ignore

    qr.call_procedure(
        endpoint="gds.degree.write",
        params=CallParameters(
            graph_name="g",
            config={"arrowConfiguration": {"batchSize": 1000}, "jobId": "my-job"},
        ),
    )

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"writeToResultStore": True, "jobId": "my-job"},
    }
    assert (
        db_query_runner.last_query() == "CALL gds.arrow.write($graphName, $databaseName, $jobId, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "jobId": "my-job",
        "arrowConfiguration": {
            "encrypted": False,
            "host": "myHost",
            "port": "1234",
            "token": "myToken",
            "batchSize": 1000,
        },
    }


def test_arrow_and_write_configuration_graph_write() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version, result_mock=DataFrame([{"version": "v1"}]))
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(gds_query_runner, db_query_runner, FakeArrowClient(), True)  # type: ignore

    qr.call_procedure(
        endpoint="gds.graph.nodeProperties.write",
        params=CallParameters(
            graph_name="g",
            properties=[],
            entities=[],
            config={"arrowConfiguration": {"batchSize": 42}, "jobId": "my-job"},
        ),
    )

    assert (
        gds_query_runner.last_query()
        == "CALL gds.graph.nodeProperties.write($graph_name, $properties, $entities, $config)"
    )
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "entities": [],
        "properties": [],
        "config": {"writeToResultStore": True, "jobId": "my-job"},
    }
    assert (
        db_query_runner.last_query() == "CALL gds.arrow.write($graphName, $databaseName, $jobId, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "jobId": "my-job",
        "arrowConfiguration": {
            "encrypted": False,
            "host": "myHost",
            "port": "1234",
            "token": "myToken",
            "batchSize": 42,
        },
    }
