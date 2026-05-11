from unittest.mock import MagicMock

import pytest
from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import (
    AuthenticatedArrowClient,
    ConnectionInfo,
)
from graphdatascience.query_runner.protocol.arrow_config import build_arrow_config
from graphdatascience.query_runner.protocol.project_protocols import (
    ProjectProtocol,
    ProjectProtocolV3,
    ProjectProtocolV4,
)
from graphdatascience.query_runner.protocol.status import Status
from graphdatascience.query_runner.termination_flag import TerminationFlagNoop
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.session.dbms.protocol_version import ProtocolVersion
from tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def arrow_client() -> MagicMock:
    client = MagicMock(spec=AuthenticatedArrowClient)
    client.advertised_connection_info.return_value = ConnectionInfo(host="arrow.host", port=1234, encrypted=True)
    client.request_token.return_value = "some-token"
    return client


@pytest.fixture
def qr() -> CollectingQueryRunner:
    return CollectingQueryRunner(ServerVersion(2, 10, 0))


def test_select_returns_v3(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = ProjectProtocol.select(ProtocolVersion.V3, arrow_client, qr, TerminationFlagNoop())
    assert isinstance(protocol, ProjectProtocolV3)


def test_select_returns_v4(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = ProjectProtocol.select(ProtocolVersion.V4, arrow_client, qr, TerminationFlagNoop())
    assert isinstance(protocol, ProjectProtocolV4)


def test_select_unsupported_version_raises(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    with pytest.raises(KeyError):
        ProjectProtocol.select(ProtocolVersion.V1, arrow_client, qr, TerminationFlagNoop())


def test_arrow_config_uses_advertised_connection_info(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    config = build_arrow_config(protocol._arrow_client, 100)

    assert config == {
        "host": "arrow.host",
        "port": 1234,
        "token": "some-token",
        "encrypted": True,
        "batchSize": 100,
    }


def test_arrow_config_falls_back_to_ignored_token_when_none(
    qr: CollectingQueryRunner,
) -> None:
    arrow_client = MagicMock(spec=AuthenticatedArrowClient)
    arrow_client.advertised_connection_info.return_value = ConnectionInfo(host="arrow.host", port=1234, encrypted=False)
    arrow_client.request_token.return_value = None

    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    config = build_arrow_config(protocol._arrow_client, None)

    assert config["token"] == "IGNORED"
    assert config["batchSize"] is None
    assert config["encrypted"] is False


def test_v3_store_projection_raises_not_implemented(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

    with pytest.raises(NotImplementedError):
        protocol.run_store_projection(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )


def test_v4_run_cypher_projection_dispatches_expected_query_and_params(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.project.cypher.v4",
        DataFrame([{"jobId": "job-1", "host": "member-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status",
        DataFrame([{"status": Status.DONE.name, "error": None, "result": {"nodeCount": 42}}]),
    )

    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_cypher_projection(
        graph_name="myGraph",
        query="MATCH (n) RETURN n",
        job_id="my-job",
        concurrency=4,
        undirected_relationship_types=["REL"],
        inverse_indexed_relationship_types=["REL2"],
        batch_size=200,
    )

    assert result == {"nodeCount": 42}

    start_query = qr.queries[0]
    assert "gds.arrow.project.cypher.v4" in start_query

    start_params = qr.params[0]
    assert start_params["graph_name"] == "myGraph"
    assert start_params["query"] == "MATCH (n) RETURN n"
    assert start_params["jobId"] == "my-job"
    assert start_params["configuration"] == {
        "undirectedRelationshipTypes": ["REL"],
        "inverseIndexedRelationshipTypes": ["REL2"],
        "concurrency": 4,
    }
    assert start_params["arrow_config"] == {
        "host": "arrow.host",
        "port": 1234,
        "token": "some-token",
        "encrypted": True,
        "batchSize": 200,
    }

    status_query = qr.queries[1]
    assert "gds.arrow.job.status('job-1')" in status_query


def test_v4_run_store_projection_dispatches_expected_query_and_params(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.project.store.v4",
        DataFrame([{"jobId": "store-job", "host": "member-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status",
        DataFrame([{"status": Status.DONE.name, "error": None, "result": {"nodeCount": 7}}]),
    )

    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_store_projection(
        graph_name="myGraph",
        node_label_filter=["Person", "Movie"],
        relationship_type_filter=["ACTED_IN"],
        node_properties=["age"],
        relationship_properties=["weight"],
        job_id="store-job",
        concurrency=8,
        undirected_relationship_types=["ACTED_IN"],
        inverse_indexed_relationship_types=[],
        batch_size=50,
    )

    assert result == {"nodeCount": 7}

    start_query = qr.queries[0]
    assert "gds.arrow.project.store.v4" in start_query

    start_params = qr.params[0]
    assert start_params["graph_name"] == "myGraph"
    assert start_params["node_labels"] == ["Person", "Movie"]
    assert start_params["relationship_types"] == ["ACTED_IN"]
    assert start_params["configuration"] == {
        "nodeProperties": ["age"],
        "relationshipProperties": ["weight"],
        "jobId": "store-job",
        "undirectedRelationshipTypes": ["ACTED_IN"],
        "inverseIndexedRelationshipTypes": [],
        "readConcurrency": 8,
    }
    assert start_params["arrow_config"]["host"] == "arrow.host"
    assert start_params["arrow_config"]["port"] == 1234
    assert start_params["arrow_config"]["batchSize"] == 50

    status_query = qr.queries[1]
    assert "gds.arrow.job.status('store-job')" in status_query


def test_v4_run_cypher_projection_polls_until_done(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.project.cypher.v4",
        DataFrame([{"jobId": "job-1", "host": "member-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status",
        [
            DataFrame([{"status": Status.RUNNING.name, "error": None, "result": None}]),
            DataFrame([{"status": Status.RUNNING.name, "error": None, "result": None}]),
            DataFrame([{"status": Status.DONE.name, "error": None, "result": {"nodeCount": 1}}]),
        ],
    )

    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_cypher_projection(graph_name="g", query="MATCH (n) RETURN n", job_id="my-job")

    assert result == {"nodeCount": 1}

    status_queries = [q for q in qr.queries if "gds.arrow.job.status" in q]
    assert len(status_queries) == 3


def test_v4_run_cypher_projection_raises_when_status_has_error(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.project.cypher.v4",
        DataFrame([{"jobId": "job-1", "host": "member-host", "port": 7777}]),
    )
    qr.add__mock_result(
        "gds.arrow.job.status",
        DataFrame([{"status": Status.RUNNING.name, "error": "boom", "result": None}]),
    )

    protocol = ProjectProtocolV4(arrow_client, qr, TerminationFlagNoop())

    with pytest.raises(Exception, match="boom"):
        protocol.run_cypher_projection(graph_name="g", query="MATCH (n) RETURN n", job_id="my-job")


def test_v3_run_cypher_projection_dispatches_expected_query_and_params(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.project.v3",
        [
            DataFrame([{"host": "member-host", "port": 7777}]),
            DataFrame([{"status": Status.DONE.name, "nodeCount": 42}]),
        ],
    )

    protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_cypher_projection(
        graph_name="myGraph",
        query="MATCH (n) RETURN n",
        job_id="my-job",
        concurrency=4,
        undirected_relationship_types=["REL"],
        inverse_indexed_relationship_types=["REL2"],
        batch_size=200,
    )

    assert result == {"status": Status.DONE.name, "nodeCount": 42}

    assert len(qr.queries) == 2
    assert all("gds.arrow.project.v3" in q for q in qr.queries)

    expected_params = {
        "graph_name": "myGraph",
        "query": "MATCH (n) RETURN n",
        "jobId": "my-job",
        "configuration": {
            "undirectedRelationshipTypes": ["REL"],
            "inverseIndexedRelationshipTypes": ["REL2"],
            "concurrency": 4,
        },
        "arrow_config": {
            "host": "arrow.host",
            "port": 1234,
            "token": "some-token",
            "encrypted": True,
            "batchSize": 200,
        },
    }
    assert qr.params[0] == expected_params
    assert qr.params[1] == expected_params


def test_v3_run_cypher_projection_polls_until_done(arrow_client: MagicMock, qr: CollectingQueryRunner) -> None:
    qr.add__mock_result(
        "gds.arrow.project.v3",
        [
            DataFrame([{"host": "member-host", "port": 7777}]),
            DataFrame([{"status": Status.RUNNING.name, "nodeCount": 0}]),
            DataFrame([{"status": Status.RUNNING.name, "nodeCount": 0}]),
            DataFrame([{"status": Status.DONE.name, "nodeCount": 1}]),
        ],
    )

    protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_cypher_projection(graph_name="g", query="MATCH (n) RETURN n", job_id="my-job")

    assert result == {"status": Status.DONE.name, "nodeCount": 1}
    assert len(qr.queries) == 4
    assert all("gds.arrow.project.v3" in q for q in qr.queries)


def test_v3_run_cypher_projection_defaults_port_when_missing(
    arrow_client: MagicMock, qr: CollectingQueryRunner
) -> None:
    qr.add__mock_result(
        "gds.arrow.project.v3",
        [
            DataFrame([{"host": "member-host", "other": "x"}]),
            DataFrame([{"status": Status.DONE.name, "nodeCount": 0}]),
        ],
    )

    protocol = ProjectProtocolV3(arrow_client, qr, TerminationFlagNoop())

    result = protocol.run_cypher_projection(graph_name="g", query="MATCH (n) RETURN n", job_id="my-job")

    assert result == {"status": Status.DONE.name, "nodeCount": 0}
    assert len(qr.queries) == 2
