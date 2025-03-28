import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.procedure_surface.cypher.wcc_proc_runner import WccCypherEndpoints
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


@pytest.fixture
def query_runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(ServerVersion(2, 16, 0))


@pytest.fixture
def wcc_endpoints(query_runner: CollectingQueryRunner) -> WccCypherEndpoints:
    return WccCypherEndpoints(query_runner)


@pytest.fixture
def graph(query_runner: CollectingQueryRunner) -> Graph:
    return Graph("test_graph", query_runner)


def test_mutate_basic(wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    wcc_endpoints.mutate(graph, "componentId")

    assert len(query_runner.queries) == 1
    assert "gds.wcc.mutate" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["mutateProperty"] == "componentId"
    assert "jobId" in config


def test_mutate_with_optional_params(
    wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    wcc_endpoints.mutate(
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


def test_stats_basic(wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    wcc_endpoints.stats(graph)

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stats" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stats_with_optional_params(
    wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    wcc_endpoints.stats(
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


def test_stream_basic(wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    wcc_endpoints.stream(graph)

    assert len(query_runner.queries) == 1
    assert "gds.wcc.stream" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert "jobId" in config


def test_stream_with_optional_params(
    wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
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


def test_write_basic(wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner) -> None:
    wcc_endpoints.write(graph, "componentId")

    assert len(query_runner.queries) == 1
    assert "gds.wcc.write" in query_runner.queries[0]
    params = query_runner.params[0]
    assert params["graph_name"] == "test_graph"
    config = params["config"]
    assert config["writeProperty"] == "componentId"
    assert "jobId" in config


def test_write_with_optional_params(
    wcc_endpoints: WccCypherEndpoints, graph: Graph, query_runner: CollectingQueryRunner
) -> None:
    wcc_endpoints.write(
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
        write_to_result_store=True,
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
        "writeToResultStore": True,
    }
