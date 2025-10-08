import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.louvain_endpoints import (
    LouvainMutateResult,
    LouvainStatsResult,
    LouvainWriteResult,
)
from graphdatascience.procedure_surface.cypher.community.louvain_cypher_endpoints import LouvainCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def louvain_endpoints(query_runner: CollectingQueryRunner) -> LouvainCypherEndpoints:
    return LouvainCypherEndpoints(query_runner)


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 6,
        "modularity": 0.5,
        "modularities": [0.42],
        "ranLevels": 1337,
        "mutateMillis": 50,
        "communityCount": 2,
        "preProcessingMillis": 15,
        "computeMillis": 25,
        "postProcessingMillis": 10,
        "communityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"louvain.mutate": pd.DataFrame([result])})

    result_obj = LouvainCypherEndpoints(query_runner).mutate(graph, "communityId")

    assert len(query_runner.queries) == 1
    assert "gds.louvain.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "communityId"
    assert "jobId" in config

    assert isinstance(result_obj, LouvainMutateResult)
    assert result_obj.node_properties_written == 6
    assert result_obj.modularity == 0.5
    assert result_obj.modularities == [0.42]
    assert result_obj.ran_levels == 1337
    assert result_obj.mutate_millis == 50
    assert result_obj.community_count == 2
    assert result_obj.pre_processing_millis == 15
    assert result_obj.compute_millis == 25
    assert result_obj.post_processing_millis == 10
    assert result_obj.community_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "communityCount": 2,
        "preProcessingMillis": 15,
        "computeMillis": 25,
        "postProcessingMillis": 10,
        "communityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
        "modularity": 0.5,
        "modularities": [0.42],
        "ranLevels": 1337,
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"louvain.stats": pd.DataFrame([result])})

    result_obj = LouvainCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.louvain.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, LouvainStatsResult)
    assert result_obj.community_count == 2
    assert result_obj.pre_processing_millis == 15
    assert result_obj.compute_millis == 25
    assert result_obj.post_processing_millis == 10
    assert result_obj.community_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stream_basic(
    louvain_endpoints: LouvainCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    louvain_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.louvain.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "communityCount": 2,
        "preProcessingMillis": 15,
        "computeMillis": 25,
        "writeMillis": 20,
        "postProcessingMillis": 10,
        "nodePropertiesWritten": 6,
        "modularity": 0.5,
        "modularities": [0.42],
        "ranLevels": 1337,
        "communityDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"louvain.write": pd.DataFrame([result])})

    result_obj = LouvainCypherEndpoints(query_runner).write(graph, "communityId")

    assert len(query_runner.queries) == 1
    assert "gds.louvain.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "communityId"
    assert "jobId" in config

    assert isinstance(result_obj, LouvainWriteResult)
    assert result_obj.community_count == 2
    assert result_obj.pre_processing_millis == 15
    assert result_obj.compute_millis == 25
    assert result_obj.write_millis == 20
    assert result_obj.post_processing_millis == 10
    assert result_obj.node_properties_written == 6
    assert result_obj.modularity == 0.5
    assert result_obj.modularities == [0.42]
    assert result_obj.ran_levels == 1337
    assert result_obj.community_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"louvain.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = LouvainCypherEndpoints(query_runner).estimate(G=graph)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.louvain.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"
