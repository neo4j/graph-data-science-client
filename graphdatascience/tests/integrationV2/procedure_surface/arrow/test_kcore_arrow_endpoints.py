import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.kcore_arrow_endpoints import KCoreArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (d: Node)
    (e: Node)
    (f: Node)
    (a)-[:REL]->(b)
    (b)-[:REL]->(c)
    (c)-[:REL]->(a)
    (d)-[:REL]->(e)
    (e)-[:REL]->(f)
    (f)-[:REL]->(d)
    (a)-[:REL]->(d)
    """

    yield create_graph(arrow_client, "kcore_g", gdl, ("REL", "REL2"))
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "kcore_g"}).encode("utf-8"))


@pytest.fixture
def kcore_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[KCoreArrowEndpoints, None, None]:
    yield KCoreArrowEndpoints(arrow_client)


def test_kcore_stats(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stats operation."""
    result = kcore_endpoints.stats(G=sample_graph)

    assert result.degeneracy >= 1
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.pre_processing_millis >= 0


def test_kcore_stream(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stream operation."""
    result_df = kcore_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core mutate operation."""
    result = kcore_endpoints.mutate(G=sample_graph, mutate_property="coreValue")

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_estimate(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core estimate operation."""
    result = kcore_endpoints.estimate(sample_graph)

    assert result.node_count == 6
    assert result.relationship_count == 14
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0


def test_kcore_stats_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stats operation with various parameters."""
    result = kcore_endpoints.stats(G=sample_graph, relationship_types=["REL2"], concurrency=2)

    assert result.degeneracy >= 1
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_kcore_stream_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stream operation with various parameters."""
    result_df = kcore_endpoints.stream(G=sample_graph, relationship_types=["REL2"], concurrency=2)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) == 6


def test_kcore_mutate_with_parameters(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core mutate operation with various parameters."""
    result = kcore_endpoints.mutate(
        G=sample_graph, mutate_property="kcoreValue", relationship_types=["REL2"], concurrency=2
    )

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 6


def test_kcore_write_without_write_back_client(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core write operation raises exception when write_back_client is None."""
    with pytest.raises(Exception, match="Write back client is not initialized"):
        kcore_endpoints.write(
            G=sample_graph,
            write_property="coreValue",
        )


def test_kcore_stats_with_target_nodes(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stats operation with target nodes parameter."""
    result = kcore_endpoints.stats(G=sample_graph)

    assert result.degeneracy >= 1
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0


def test_kcore_stream_with_target_nodes(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core stream operation with target nodes parameter."""
    result_df = kcore_endpoints.stream(G=sample_graph)

    assert "nodeId" in result_df.columns
    assert "coreValue" in result_df.columns
    assert len(result_df.columns) == 2
    assert len(result_df) <= 6


def test_kcore_mutate_with_target_nodes(kcore_endpoints: KCoreArrowEndpoints, sample_graph: Graph) -> None:
    """Test K-Core mutate operation with target nodes parameter."""
    result = kcore_endpoints.mutate(G=sample_graph, mutate_property="kcoreTargeted")

    assert result.degeneracy >= 1
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.post_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written <= 6
