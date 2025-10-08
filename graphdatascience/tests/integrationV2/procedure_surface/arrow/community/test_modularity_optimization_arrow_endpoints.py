from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.community.modularity_optimization_endpoints import (
    ModularityOptimizationWriteResult,
)
from graphdatascience.procedure_surface.arrow.community.modularity_optimization_arrow_endpoints import (
    ModularityOptimizationArrowEndpoints,
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
            (f: Node),
            (a)-[:REL]->(b),
            (a)-[:REL]->(c),
            (b)-[:REL]->(c),
            (d)-[:REL]->(e),
            (d)-[:REL]->(f),
            (e)-[:REL]->(f),
            (a)-[:REL]->(d)
        """


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[GraphV2, None, None]:
    with create_graph(arrow_client, "g", graph) as G:
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
    ) as g:
        yield g


@pytest.fixture
def modularity_optimization_endpoints(
    arrow_client: AuthenticatedArrowClient,
) -> Generator[ModularityOptimizationArrowEndpoints, None, None]:
    yield ModularityOptimizationArrowEndpoints(arrow_client, show_progress=False)


def test_modularity_optimization_stats(
    modularity_optimization_endpoints: ModularityOptimizationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = modularity_optimization_endpoints.stats(G=sample_graph, max_iterations=1)

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert isinstance(result.configuration, dict)


def test_modularity_optimization_stream(
    modularity_optimization_endpoints: ModularityOptimizationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result_df = modularity_optimization_endpoints.stream(G=sample_graph, max_iterations=1)

    assert len(result_df) == 6  # 6 nodes in the graph
    assert "nodeId" in result_df.columns
    assert "communityId" in result_df.columns


def test_modularity_optimization_mutate(
    modularity_optimization_endpoints: ModularityOptimizationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = modularity_optimization_endpoints.mutate(
        G=sample_graph,
        max_iterations=1,
        mutate_property="modularity_optimization_community",
    )

    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.mutate_millis >= 0
    assert result.nodes == 6
    assert isinstance(result.configuration, dict)


@pytest.mark.db_integration
def test_modularity_optimization_write(
    arrow_client: AuthenticatedArrowClient, db_graph: GraphV2, query_runner: QueryRunner
) -> None:
    from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient

    endpoints = ModularityOptimizationArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))

    result = endpoints.write(
        G=db_graph,
        max_iterations=1,
        write_property="modularity_optimization_community_write",
    )

    assert isinstance(result, ModularityOptimizationWriteResult)
    assert result.ran_iterations > 0
    assert result.did_converge in [True, False]
    assert result.compute_millis > 0
    assert result.pre_processing_millis >= 0
    assert result.write_millis >= 0
    assert result.nodes == 6
    assert isinstance(result.configuration, dict)


def test_modularity_optimization_estimate(
    modularity_optimization_endpoints: ModularityOptimizationArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = modularity_optimization_endpoints.estimate(sample_graph, max_iterations=1)

    assert result.node_count == 6
    assert result.relationship_count == 7
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
