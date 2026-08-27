import pandas as pd
import pytest

from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.centrality.hits_endpoints import (
    HitsMutateResult,
    HitsStatsResult,
    HitsWriteResult,
)
from graphdatascience.procedure_surface.cypher.centrality.hits_cypher_endpoints import HitsCypherEndpoints
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from tests.unit.procedure_surface.cypher.conftest import estimate_mock_result


@pytest.fixture
def hits_endpoints(query_runner: CollectingQueryRunner) -> HitsCypherEndpoints:
    return HitsCypherEndpoints(query_runner)


def test_mutate_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 10,
        "mutateMillis": 42,
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"hits.mutate": pd.DataFrame([result])})

    result_obj = HitsCypherEndpoints(query_runner).mutate(graph, mutate_property="hits")

    assert len(query_runner.queries) == 1
    assert "gds.hits.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "hits"
    assert "jobId" in config

    assert isinstance(result_obj, HitsMutateResult)
    assert result_obj.node_properties_written == 10
    assert result_obj.mutate_millis == 42
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True


def test_mutate_with_optional_params(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 10,
        "mutateMillis": 42,
        "ranIterations": 5,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"hits.mutate": pd.DataFrame([result])})

    HitsCypherEndpoints(query_runner).mutate(
        graph,
        mutate_property="hits",
        hits_iterations=5,
        auth_property="a",
        hub_property="h",
        partitioning="RANGE",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
    )

    assert len(query_runner.queries) == 1
    params = query_runner.params[0]
    assert params["config"] == {
        "mutateProperty": "hits",
        "hitsIterations": 5,
        "authProperty": "a",
        "hubProperty": "h",
        "partitioning": "RANGE",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
    }


def test_stats_basic(graph: Graph) -> None:
    result = {
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"hits.stats": pd.DataFrame([result])})

    result_obj = HitsCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.hits.stats" in query_runner.queries[0]
    assert "jobId" in query_runner.params[0]["config"]

    assert isinstance(result_obj, HitsStatsResult)
    assert result_obj.ran_iterations == 20
    assert result_obj.did_converge is True


def test_stream_basic(hits_endpoints: HitsCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    hits_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.hits.stream" in query_runner.queries[0]
    config = query_runner.params[0]["config"]
    assert config["hitsIterations"] == 20
    assert config["authProperty"] == "auth"
    assert config["hubProperty"] == "hub"
    assert "jobId" in config


def test_write_basic(graph: Graph) -> None:
    result = {
        "nodePropertiesWritten": 10,
        "writeMillis": 42,
        "ranIterations": 20,
        "didConverge": True,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"hits.write": pd.DataFrame([result])})

    result_obj = HitsCypherEndpoints(query_runner).write(graph, write_property="hits")

    assert len(query_runner.queries) == 1
    assert "gds.hits.write" in query_runner.queries[0]
    config = query_runner.params[0]["config"]
    assert config["writeProperty"] == "hits"
    assert "jobId" in config

    assert isinstance(result_obj, HitsWriteResult)
    assert result_obj.node_properties_written == 10
    assert result_obj.write_millis == 42


def test_estimate_basic(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"hits.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    HitsCypherEndpoints(query_runner).estimate(graph, hits_iterations=5)

    assert len(query_runner.queries) == 1
    assert "gds.hits.stats.estimate" in query_runner.queries[0]
    algo_config = query_runner.params[0]["algoConfig"]
    assert algo_config["hitsIterations"] == 5
