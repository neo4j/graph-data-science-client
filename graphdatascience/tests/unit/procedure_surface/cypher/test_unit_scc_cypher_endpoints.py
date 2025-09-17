import pandas as pd
import pytest

from graphdatascience.procedure_surface.api.catalog.graph_api import Graph
from graphdatascience.procedure_surface.api.scc_endpoints import SccMutateResult, SccStatsResult, SccWriteResult
from graphdatascience.procedure_surface.cypher.catalog.graph_backend_cypher import wrap_graph
from graphdatascience.procedure_surface.cypher.scc_cypher_endpoints import SccCypherEndpoints
from graphdatascience.tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner
from graphdatascience.tests.unit.procedure_surface.cypher.conftests import estimate_mock_result


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION)


@pytest.fixture
def scc_endpoints(query_runner: CollectingQueryRunner) -> SccCypherEndpoints:
    return SccCypherEndpoints(query_runner)


@pytest.fixture
def graph() -> Graph:
    return wrap_graph("test_graph", CollectingQueryRunner(DEFAULT_SERVER_VERSION))


def test_mutate_basic(scc_endpoints: SccCypherEndpoints, graph: Graph) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "mutateMillis": 15,
        "nodePropertiesWritten": 5,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.mutate": pd.DataFrame([result])})

    result_obj = SccCypherEndpoints(query_runner).mutate(graph, "componentId")

    assert isinstance(result_obj, SccMutateResult)
    assert result_obj.component_count == 3
    assert result_obj.node_properties_written == 5


def test_mutate_with_optional_params(graph: Graph) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "mutateMillis": 15,
        "nodePropertiesWritten": 5,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.mutate": pd.DataFrame([result])})

    SccCypherEndpoints(query_runner).mutate(
        graph,
        "componentId",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        consecutive_ids=True,
    )

    assert len(query_runner.queries) == 1
    assert "gds.scc.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "mutateProperty": "componentId",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "consecutiveIds": True,
    }


def test_stats_basic(scc_endpoints: SccCypherEndpoints, graph: Graph) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.stats": pd.DataFrame([result])})

    result_obj = SccCypherEndpoints(query_runner).stats(graph)

    assert isinstance(result_obj, SccStatsResult)
    assert result_obj.component_count == 3


def test_stats_with_optional_params(graph: Graph) -> None:
    result = {
        "componentCount": 3,
        "preProcessingMillis": 10,
        "computeMillis": 20,
        "postProcessingMillis": 12,
        "componentDistribution": {"foo": 42},
        "configuration": {"bar": 1337},
    }

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.stats": pd.DataFrame([result])})

    SccCypherEndpoints(query_runner).stats(
        graph,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        consecutive_ids=True,
    )

    assert len(query_runner.queries) == 1
    assert "gds.scc.stats" in query_runner.queries[0]
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
        "consecutiveIds": True,
    }


def test_stream_basic(scc_endpoints: SccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    result_data = pd.DataFrame({"nodeId": [1, 2, 3], "componentId": [1, 1, 2]})
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.stream": result_data})

    result = SccCypherEndpoints(query_runner).stream(graph)

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 3
    assert "nodeId" in result.columns
    assert "componentId" in result.columns


def test_stream_with_optional_params(
    scc_endpoints: SccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    result_data = pd.DataFrame({"nodeId": [1, 2, 3], "componentId": [1, 1, 2]})
    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.stream": result_data})

    SccCypherEndpoints(query_runner).stream(
        graph,
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        consecutive_ids=True,
    )

    assert len(query_runner.queries) == 1
    assert "gds.scc.stream" in query_runner.queries[0]
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
        "consecutiveIds": True,
    }


def test_write_basic(scc_endpoints: SccCypherEndpoints, graph: Graph) -> None:
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

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.write": pd.DataFrame([result])})

    result_obj = SccCypherEndpoints(query_runner).write(graph, "componentId")

    assert isinstance(result_obj, SccWriteResult)
    assert result_obj.component_count == 3
    assert result_obj.node_properties_written == 5


def test_write_with_optional_params(graph: Graph) -> None:
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

    query_runner = CollectingQueryRunner(DEFAULT_SERVER_VERSION, {"scc.write": pd.DataFrame([result])})

    SccCypherEndpoints(query_runner).write(
        graph,
        "componentId",
        relationship_types=["REL"],
        node_labels=["Person"],
        sudo=True,
        log_progress=True,
        username="neo4j",
        concurrency=4,
        job_id="test-job",
        consecutive_ids=True,
        write_concurrency=4,
    )

    assert len(query_runner.queries) == 1
    assert "gds.scc.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    assert params["config"] == {
        "writeProperty": "componentId",
        "relationshipTypes": ["REL"],
        "nodeLabels": ["Person"],
        "sudo": True,
        "logProgress": True,
        "username": "neo4j",
        "concurrency": 4,
        "jobId": "test-job",
        "consecutiveIds": True,
        "writeConcurrency": 4,
    }


def test_estimate_with_graph(graph: Graph) -> None:
    query_runner = CollectingQueryRunner(
        DEFAULT_SERVER_VERSION, {"scc.stats.estimate": pd.DataFrame([estimate_mock_result()])}
    )

    estimate = SccCypherEndpoints(query_runner).estimate(G=graph)

    assert estimate.node_count == 100
    assert estimate.relationship_count == 200
