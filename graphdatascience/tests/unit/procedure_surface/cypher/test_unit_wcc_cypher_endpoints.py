import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.wcc_endpoints import WccMutateResult, WccStatsResult, WccWriteResult
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import wrap_graph
from graphdatascience.procedure_surface.cypher.wcc_cypher_endpoints import WccCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION)


@pytest.fixture
def wcc_endpoints(query_runner: CollectingQueryRunner) -> WccCypherEndpoints:
    return WccCypherEndpoints(query_runner)


@pytest.fixture
def graph(query_runner: CollectingQueryRunner) -> GraphV2:
    return wrap_graph("test_graph", query_runner)


def test_mutate_basic(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.mutate": pd.DataFrame([result])})

    result_obj = WccCypherEndpoints(query_runner).mutate(graph, "componentId")

    assert len(query_runner.queries) == 1
    assert "gds.wcc.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "componentId"
    assert "jobId" in config

    assert isinstance(result_obj, WccMutateResult)
    assert result_obj.node_properties_written == 5
    assert result_obj.mutate_millis == 42
    assert result_obj.component_count == 3
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.component_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_mutate_with_optional_params(graph: GraphV2) -> None:
    result = {
        "nodePropertiesWritten": 5,
        "mutateMillis": 42,
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.mutate": pd.DataFrame([result])})

    WccCypherEndpoints(query_runner).mutate(
        graph,
        "componentId",
        threshold=0.5,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        seed_property="seed",
        consecutive_ids=True,
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.wcc.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "componentId",
        "threshold": 0.5,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "seedProperty": "seed",
        "consecutiveIds": True,
        "relationshipWeightProperty": "weight",
    }


def test_stats_basic(graph: GraphV2) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.stats": pd.DataFrame([result])})

    result_obj = WccCypherEndpoints(query_runner).stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config

    assert isinstance(result_obj, WccStatsResult)
    assert result_obj.component_count == 3
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.post_processing_millis == 12
    assert result_obj.component_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_stats_with_optional_params(graph: GraphV2) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.stats": pd.DataFrame([result])})

    WccCypherEndpoints(query_runner).stats(
        graph,
        threshold=0.5,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        seed_property="seed",
        consecutive_ids=True,
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "threshold": 0.5,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "seedProperty": "seed",
        "consecutiveIds": True,
        "relationshipWeightProperty": "weight",
    }


def test_stream_basic(wcc_endpoints: WccCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner) -> None:
    wcc_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    wcc_endpoints: WccCypherEndpoints, graph: GraphV2, query_runner: CollectingQueryRunner
) -> None:
    wcc_endpoints.stream(
        graph,
        min_component_size=2,
        threshold=0.5,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        seed_property="seed",
        consecutive_ids=True,
        relationship_weight_property="weight",
    )

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "minComponentSize": 2,
        "threshold": 0.5,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "seedProperty": "seed",
        "consecutiveIds": True,
        "relationshipWeightProperty": "weight",
    }


def test_write_basic(graph: GraphV2) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 15,
        "postProcessingMillis": 12,
        "nodePropertiesWritten": 5,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.write": pd.DataFrame([result])})

    result_obj = WccCypherEndpoints(query_runner).write(graph, "componentId")

    assert len(query_runner.queries) == 1
    assert "gds.wcc.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "componentId"
    assert "jobId" in config

    assert isinstance(result_obj, WccWriteResult)
    assert result_obj.component_count == 3
    assert result_obj.pre_processing_millis == 10
    assert result_obj.compute_millis == 20
    assert result_obj.write_millis == 15
    assert result_obj.post_processing_millis == 12
    assert result_obj.node_properties_written == 5
    assert result_obj.component_distribution == {"foo": 42}
    assert result_obj.configuration == {"bar": 1337}


def test_write_with_optional_params(graph: GraphV2) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "writeMillis": 15,
        "postProcessingMillis": 12,
        "nodePropertiesWritten": 5,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"wcc.write": pd.DataFrame([result])})

    WccCypherEndpoints(query_runner).write(
        graph,
        "componentId",
        min_component_size=2,
        threshold=0.5,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        seed_property="seed",
        consecutive_ids=True,
        relationship_weight_property="weight",
        write_concurrency=4,
    )

    assert len(query_runner.queries) == 1
    assert "gds.wcc.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "componentId",
        "minComponentSize": 2,
        "threshold": 0.5,
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "seedProperty": "seed",
        "consecutiveIds": True,
        "relationshipWeightProperty": "weight",
        "writeConcurrency": 4,
    }


def test_estimate_with_graph_name(graph: GraphV2) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"wcc.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = WccCypherEndpoints(query_runner).estimate(G=graph)

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == "test_graph"


def test_estimate_with_projection_config() -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"wcc.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = WccCypherEndpoints(query_runner).estimate(G={"foo": "bar"})

    assert estimate.node_count == 100

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stats.estimate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graphNameOrConfiguration"] == {"foo": "bar"}
