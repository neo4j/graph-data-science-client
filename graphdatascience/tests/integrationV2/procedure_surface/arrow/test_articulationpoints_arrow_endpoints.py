import json
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.articulationpoints_endpoints import (
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
)
from graphdatascience.procedure_surface.arrow.articulationpoints_arrow_endpoints import (
    ArticulationPointsArrowEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(c)
    (b)-[:REL]->(c)
    """

    yield create_graph(arrow_client, "g", gdl, undirected=("REL", "UNDIRECTED_REL"))
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "g"}).encode("utf-8"))


@pytest.fixture
def articulationpoints_endpoints(arrow_client: AuthenticatedArrowClient) -> ArticulationPointsArrowEndpoints:
    return ArticulationPointsArrowEndpoints(arrow_client)


def test_articulationpoints_mutate(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: Graph
) -> None:
    """Test ArticulationPoints mutate operation."""
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
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: Graph
) -> None:
    """Test ArticulationPoints stats operation."""
    result = articulationpoints_endpoints.stats(sample_graph)

    assert isinstance(result, ArticulationPointsStatsResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0


def test_articulationpoints_stream_not_implemented(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: Graph
) -> None:
    """Test that ArticulationPoints stream raises NotImplementedError."""
    with pytest.raises(
        NotImplementedError, match="Stream mode is not supported for ArticulationPoints arrow endpoints"
    ):
        articulationpoints_endpoints.stream(sample_graph)


def test_articulationpoints_estimate(
    articulationpoints_endpoints: ArticulationPointsArrowEndpoints, sample_graph: Graph
) -> None:
    """Test ArticulationPoints memory estimation."""
    result = articulationpoints_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 4  # undirected
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
