from typing import Tuple

from pandas import DataFrame

from graphdatascience import ServerVersion
from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbArrowQueryRunner,
)
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


class FakeArrowClient:

    def connection_info(self) -> Tuple[str, str]:
        return "myHost", "1234"

    def request_token(self) -> str:
        return "myToken"


def test_extracts_parameters_projection() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version)
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = AuraDbArrowQueryRunner(gds_query_runner, db_query_runner, FakeArrowClient(), False)  # type: ignore

    qr.call_procedure(
        endpoint="gds.arrow.project",
        params=CallParameters(
            graph_name="g",
            query="RETURN 1",
            concurrency=2,
            undirRels=[],
            inverseRels=[],
            arrow_configuration={"batchSize": 100},
        ),
    )

    # doesn't run anything on GDS
    assert gds_query_runner.last_query() == ""
    assert gds_query_runner.last_params() == {}
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.project($graph_name, $query, $concurrency, $undirRels, $inverseRels, $arrow_configuration)"
    )
    assert db_query_runner.last_params() == {
        "graph_name": "g",
        "query": "RETURN 1",
        "concurrency": 2,
        "undirRels": [],
        "inverseRels": [],
        "arrow_configuration": {
            "encrypted": False,
            "host": "myHost",
            "port": "1234",
            "token": "myToken",
            "batchSize": 100,
        },
    }


def test_extracts_parameters_algo_write() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version)
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = AuraDbArrowQueryRunner(gds_query_runner, db_query_runner, FakeArrowClient(), False)  # type: ignore

    qr.call_procedure(endpoint="gds.degree.write", params=CallParameters(graph_name="g", config={}))

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"writeToResultStore": True},
    }
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.write($graphName, $databaseName, $writeConfiguration, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "writeConfiguration": {"nodeLabels": ["*"]},
        "arrowConfiguration": {"encrypted": False, "host": "myHost", "port": "1234", "token": "myToken"},
    }


def test_arrow_and_write_configuration() -> None:
    version = ServerVersion(2, 7, 0)
    db_query_runner = CollectingQueryRunner(version)
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = AuraDbArrowQueryRunner(gds_query_runner, db_query_runner, FakeArrowClient(), False)  # type: ignore

    qr.call_procedure(
        endpoint="gds.degree.write",
        params=CallParameters(
            graph_name="g",
            config={"arrowConfiguration": {"batchSize": 1000}, "writeConfiguration": {"writeMode": "FOOBAR"}},
        ),
    )

    assert gds_query_runner.last_query() == "CALL gds.degree.write($graph_name, $config)"
    assert gds_query_runner.last_params() == {
        "graph_name": "g",
        "config": {"writeToResultStore": True},
    }
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.write($graphName, $databaseName, $writeConfiguration, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "writeConfiguration": {"nodeLabels": ["*"], "writeMode": "FOOBAR"},
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
    db_query_runner = CollectingQueryRunner(version)
    gds_query_runner = CollectingQueryRunner(version)
    gds_query_runner.set__mock_result(DataFrame([{"databaseLocation": "remote"}]))
    qr = AuraDbArrowQueryRunner(gds_query_runner, db_query_runner, FakeArrowClient(), False)  # type: ignore

    qr.call_procedure(
        endpoint="gds.graph.nodeProperties.write",
        params=CallParameters(
            graph_name="g",
            properties=[],
            entities=[],
            config={"arrowConfiguration": {"batchSize": 42}, "writeConfiguration": {"writeMode": "FOOBAR"}},
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
        "config": {"writeToResultStore": True},
    }
    assert (
        db_query_runner.last_query()
        == "CALL gds.arrow.write($graphName, $databaseName, $writeConfiguration, $arrowConfiguration)"
    )
    assert db_query_runner.last_params() == {
        "graphName": "g",
        "databaseName": "dummy",
        "writeConfiguration": {"nodeLabels": [], "nodeProperties": [], "writeMode": "FOOBAR"},
        "arrowConfiguration": {
            "encrypted": False,
            "host": "myHost",
            "port": "1234",
            "token": "myToken",
            "batchSize": 42,
        },
    }
