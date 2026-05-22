import pandas as pd
import pytest

from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.random_walk_endpoints import (
    RandomWalkMutateResult,
    RandomWalkStatsResult,
)
from graphdatascience.procedure_surface.cypher.pathfinding.random_walk_cypher_endpoints import (
    RandomWalkCypherEndpoints,
)
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def random_walk_endpoints(query_runner: CollectingQueryRunner) -> RandomWalkCypherEndpoints:
    return RandomWalkCypherEndpoints(query_runner)


def test_stream_basic(
    random_walk_endpoints: RandomWalkCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    random_walk_endpoints.stream(graph, source_nodes=[1, 2], walk_length=3)

    assert len(query_runner.queries) == 1
    assert "gds.randomWalk.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["sourceNodes"] == [1, 2]
    assert config["walkLength"] == 3
    assert "jobId" in config


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 7,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "mutateMillis": 42,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"randomWalk.mutate": pd.DataFrame([result])})

    result_obj = RandomWalkCypherEndpoints(query_runner).mutate(graph, mutate_property="walks")

    assert len(query_runner.queries) == 1
    assert "gds.randomWalk.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "walks"

    assert isinstance(result_obj, RandomWalkMutateResult)
    assert result_obj.node_properties_written == 7


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"randomWalk.stats": pd.DataFrame([result])})

    result_obj = RandomWalkCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.randomWalk.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"

    assert isinstance(result_obj, RandomWalkStatsResult)
    assert result_obj.compute_millis == 20


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"randomWalk.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = RandomWalkCypherEndpoints(query_runner).estimate(G=graph, walk_length=5)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.randomWalk.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"
    assert params["algoConfig"]["walkLength"] == 5
