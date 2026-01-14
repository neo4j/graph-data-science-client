import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.bfs_endpoints import (
    BFSMutateResult,
    BFSStatsResult,
)
from graphdatascience.procedure_surface.cypher.pathfinding.bfs_cypher_endpoints import BFSCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def bfs_endpoints(query_runner: CollectingQueryRunner) -> BFSCypherEndpoints:
    return BFSCypherEndpoints(query_runner)


def test_stream_basic(bfs_endpoints: BFSCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner) -> None:
    bfs_endpoints.stream(graph, source_node=42)

    assert len(query_runner.queries) == 1
    assert "gds.bfs.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["sourceNode"] == 42
    assert "jobId" in config


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "relationshipsWritten": 5,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "mutateMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"bfs.mutate": pd.DataFrame([result])})

    result_obj = BFSCypherEndpoints(query_runner).mutate(graph, "REL", source_node=42)

    assert len(query_runner.queries) == 1
    assert "gds.bfs.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateRelationshipType"] == "REL"
    assert config["sourceNode"] == 42

    assert isinstance(result_obj, BFSMutateResult)
    assert result_obj.relationships_written == 5


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"bfs.stats": pd.DataFrame([result])})

    result_obj = BFSCypherEndpoints(query_runner).stats(graph, source_node=42)

    assert len(query_runner.queries) == 1
    assert "gds.bfs.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["sourceNode"] == 42

    assert isinstance(result_obj, BFSStatsResult)
    assert result_obj.compute_millis == 20


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"bfs.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = BFSCypherEndpoints(query_runner).estimate(G=graph, source_node=42)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.bfs.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"
    assert params["algoConfig"]["sourceNode"] == 42
