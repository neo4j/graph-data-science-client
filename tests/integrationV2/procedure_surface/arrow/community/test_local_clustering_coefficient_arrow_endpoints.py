from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.local_clustering_coefficient_endpoints import (
    LocalClusteringCoefficientWriteResult,
)
from graphdatascience.procedure_surface.arrow.community.local_clustering_coefficient_arrow_endpoints import (
    LocalClusteringCoefficientArrowEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import (
    create_graph,
    create_graph_from_db,
)

graph = """
        CREATE
            (a: Node),
            (b: Node),
            (c: Node),
            (d: Node),
            (e: Node),
            (a)-[:REL]->(b),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c),
            (c)-[:REL]->(d),
            (d)-[:REL]->(e)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph, undirected=("REL", "UNDIRECTED_REL")) as G:
        yield G


@pytest.fixture
def db_graph(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    with create_graph_from_db(
        arrow_client,
        query_runner,
        "g",
        graph,
        """
                    MATCH (n)-->(m)
                    WITH gds.graph.project.remote(n, m) as g
                    RETURN g
                """,
        undirected_relationship_types=["*"],
    ) as g:
        yield g


@pytest.fixture
def local_clustering_coefficient_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[LocalClusteringCoefficientArrowEndpoints, None, None]:
    yield LocalClusteringCoefficientArrowEndpoints(arrow_client, show_progress=False)


def test_local_clustering_coefficient_stats(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient stats operation via Arrow."""
    result = local_clustering_coefficient_endpoints.stats(G=sample_graph)

    assert result.node_count == 5
    assert result.average_clustering_coefficient >= 0.0
    assert result.average_clustering_coefficient <= 1.0
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert isinstance(result.configuration, dict)


def test_local_clustering_coefficient_stream(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient stream operation via Arrow."""
    result_df = local_clustering_coefficient_endpoints.stream(G=sample_graph)

    assert len(result_df) == 5  # 5 nodes in the graph
    assert "nodeId" in result_df.columns
    assert "localClusteringCoefficient" in result_df.columns


def test_local_clustering_coefficient_mutate(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient mutate operation via Arrow."""
    result = local_clustering_coefficient_endpoints.mutate(
        G=sample_graph,
        mutate_property="localClusteringCoefficient",
    )

    assert result.node_count == 5
    assert result.node_properties_written == 5
    assert result.average_clustering_coefficient >= 0.0
    assert result.average_clustering_coefficient <= 1.0
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_local_clustering_coefficient_write(
    arrow_client: AuthenticatedArrowClient, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    """Test Local Clustering Coefficient write operation via Arrow."""
    from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient

    endpoints = LocalClusteringCoefficientArrowEndpoints(
        arrow_client, RemoteWriteBackClient(arrow_client, query_runner)
    )

    result = endpoints.write(
        G=db_graph,
        write_property="localClusteringCoefficient",
    )

    assert isinstance(result, LocalClusteringCoefficientWriteResult)
    assert result.node_count == 5
    assert result.node_properties_written == 5
    assert result.average_clustering_coefficient >= 0.0
    assert result.average_clustering_coefficient <= 1.0
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert isinstance(result.configuration, dict)


def test_local_clustering_coefficient_estimate(
    local_clustering_coefficient_endpoints: LocalClusteringCoefficientArrowEndpoints, sample_graph: GraphV2
) -> None:
    """Test Local Clustering Coefficient estimate operation via Arrow."""
    result = local_clustering_coefficient_endpoints.estimate(sample_graph)

    assert result.node_count == 5
    assert result.relationship_count == 10
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
