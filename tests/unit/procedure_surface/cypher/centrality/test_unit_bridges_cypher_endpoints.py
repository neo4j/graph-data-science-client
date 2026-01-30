import pandas as pd
import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.centrality.bridges_cypher_endpoints import BridgesCypherEndpoints
from tests.unit.conftest import CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def bridges_endpoints(query_runner: CollectingQueryRunner) -> BridgesCypherEndpoints:
    return BridgesCypherEndpoints(query_runner)


def test_stream_basic(
    bridges_endpoints: BridgesCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    bridges_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.bridges.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    bridges_endpoints: BridgesCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    bridges_endpoints.stream(
        graph,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    assert "gds.bridges.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_estimate_with_graph_name(
    bridges_endpoints: BridgesCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    query_runner.add__mock_result("gds.bridges.stream.estimate", pd.DataFrame([estimate_mock_result()]))

    estimate = bridges_endpoints.estimate(G=graph)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.bridges.stream.estimate" in query_runner.queries[0]


def test_estimate_with_projection_config(
    bridges_endpoints: BridgesCypherEndpoints, query_runner: CollectingQueryRunner
) -> None:
    query_runner.add__mock_result("gds.bridges.stream.estimate", pd.DataFrame([estimate_mock_result()]))

    estimate = bridges_endpoints.estimate(G={"nodeCount": 42, "relationshipCount": 1337})

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.bridges.stream.estimate" in query_runner.queries[0]
