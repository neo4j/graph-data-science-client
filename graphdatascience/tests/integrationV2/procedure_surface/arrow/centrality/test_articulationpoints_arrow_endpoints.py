from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.remote_write_back_client import RemoteWriteBackClient
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.centrality.articulationpoints_endpoints import (
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from graphdatascience.procedure_surface.arrow.centrality.articulationpoints_arrow_endpoints import (
    ArticulationPointsArrowEndpoints,
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
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
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
                    WITH gds.graph.project.remote(n, m, {relationshipType: "REL"}) as g
                    RETURN g
                """,
        ["REL"],
    ) as g:
        yield g


@pytest.fixture
def articulationpoints_endpoints(arrow_client: AuthenticatedArrowClient) -> ArticulationPointsArrowEndpoints:
    return ArticulationPointsArrowEndpoints(arrow_client)


def test_articulationpoints_mutate(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = articulationpoints_endpoints.mutate(
        G=sample_graph,
        mutate_property="articulationPoint",
    )

    assert isinstance(result, ArticulationPointsMutateResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.node_properties_written == 3
    assert "articulationPoint" in result.configuration["mutateProperty"]


def test_articulationpoints_stats(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = articulationpoints_endpoints.stats(sample_graph)

    assert isinstance(result, ArticulationPointsStatsResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0


def test_articulationpoints_stream_not_implemented(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: GraphV2
) -> None:
    with pytest.raises(
        NotImplementedError, match="Stream mode is not supported for ArticulationPoints arrow endpoints"
    ):
        articulationpoints_endpoints.stream(sample_graph)


@pytest.mark.db_integration
def test_articulationpoints_write(
    arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner, db_graph: GraphV2
) -> None:
    endpoints = ArticulationPointsArrowEndpoints(arrow_client, RemoteWriteBackClient(arrow_client, query_runner))
    result = endpoints.write(G=db_graph, write_property="articulationPoint")

    assert isinstance(result, ArticulationPointsWriteResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written == 3

    assert (
        query_runner.run_cypher("MATCH (n) WHERE n.articulationPoint IS NOT NULL RETURN COUNT(*) AS count").squeeze()
        == 3
    )


def test_articulationpoints_estimate(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: GraphV2
) -> None:
    result = articulationpoints_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 4  # undirected
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
