from unittest.mock import MagicMock

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient, ConnectionInfo
from graphdatascience.arrow_client.v2.gds_arrow_client import GdsArrowClient
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.session_query_runner import SessionQueryRunner
from tests.unit.conftest import CollectingQueryRunner


class FakeArrowClient(GdsArrowClient):
    def __init__(self) -> None:
        flight_client = MagicMock(spec=AuthenticatedArrowClient)
        flight_client.advertised_connection_info.return_value = ConnectionInfo("myHost", 1234, encrypted=False)
        flight_client.request_token.return_value = "myToken"
        super().__init__(flight_client=flight_client)

    def advertised_connection_info(self) -> ConnectionInfo:
        return ConnectionInfo("myHost", 1234, encrypted=False)

    def request_token(self) -> str:
        return "myToken"


def _make_db_query_runner(version: ServerVersion, protocol: str = "v3") -> CollectingQueryRunner:
    db_query_runner = CollectingQueryRunner(version)
    db_query_runner.add__mock_result(
        "gds.session.dbms.protocol.version", DataFrame([{"version": protocol}])
    )
    db_query_runner.add__mock_result(
        "gds.arrow.write",
        DataFrame(
            [
                {
                    "status": Status.COMPLETED.name,
                    "progress": 1.0,
                    "writtenNodeProperties": 0,
                    "writtenNodeLabels": 0,
                    "writtenRelationships": 0,
                }
            ]
        ),
    )
    return db_query_runner


def test_extracts_parameters_algo_write_v3() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = _make_db_query_runner(version, protocol="v3")
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(gds_query_runner, db_query_runner, FakeArrowClient(), True)  # type: ignore

    qr.call_procedure(
        endpoint="gds.degree.write",
        params=CallParameters(graph_name="g", config={"jobId": "my-job", "concurrency": 2}),
    )

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"jobId": "my-job", "writeToResultStore": True, "concurrency": 2},
    }
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.write.v3($graphName, $jobId, $arrowConfiguration, $configuration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "jobId": "my-job",
        "arrowConfiguration": {"encrypted": False, "host": "myHost", "port": 1234, "token": "myToken"},
        "configuration": {"concurrency": 2},
    }


def test_extracts_parameters_algo_write_estimate() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = _make_db_query_runner(version, protocol="v3")
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


def test_arrow_and_write_configuration_graph_write() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = _make_db_query_runner(version, protocol="v3")
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = SessionQueryRunner.create(gds_query_runner, db_query_runner, FakeArrowClient(), True)  # type: ignore

    qr.call_procedure(
        endpoint="gds.graph.nodeProperties.write",
        params=CallParameters(
            graph_name="g",
            properties=[],
            entities=[],
            config={"jobId": "my-job"},
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
        db_query_runner.last_query()
        == "CALL gds.arrow.write.v3($graphName, $jobId, $arrowConfiguration, $configuration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "jobId": "my-job",
        "arrowConfiguration": {"encrypted": False, "host": "myHost", "port": 1234, "token": "myToken"},
        "configuration": {},
    }
