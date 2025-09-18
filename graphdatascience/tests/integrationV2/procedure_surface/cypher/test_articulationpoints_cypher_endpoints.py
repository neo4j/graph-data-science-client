from typing import Generator

import pytest
from pandas import DataFrame

from graphdatascience.procedure_surface.api.centrality.articulationpoints_endpoints import (
    ArticulationPointsMutateResult,
    ArticulationPointsStatsResult,
    ArticulationPointsWriteResult,
)
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.articulationpoints_cypher_endpoints import (
    ArticulationPointsCypherEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(c),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {}, {undirectedRelationshipTypes: ["*"]}) AS G
        RETURN G
    """

    with create_graph(
        query_runner,
        "g",
        create_statement,
        projection_query,
    ) as g:
        yield g


@pytest.fixture
def articulationpoints_endpoints(query_runner: QueryRunner) -> Generator[ArticulationPointsCypherEndpoints, None, None]:
    yield ArticulationPointsCypherEndpoints(query_runner)


def test_articulationpoints_mutate(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, sample_graph: GraphV2
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
    assert result.node_properties_written >= 0


def test_articulationpoints_stats(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test ArticulationPoints stats operation."""
    result = articulationpoints_endpoints.stats(sample_graph)

    assert isinstance(result, ArticulationPointsStatsResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0


def test_articulationpoints_stream(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test ArticulationPoints stream operation."""
    result = articulationpoints_endpoints.stream(sample_graph)

    assert isinstance(result, DataFrame)
    # Check expected columns
    expected_columns = {"nodeId", "resultingComponents"}
    assert expected_columns == set(result.columns)


def test_articulationpoints_write(
    articulationpoints_endpoints: ArticulationPointsCypherEndpoints, sample_graph: GraphV2
) -> None:
    """Test ArticulationPoints write operation."""
    result = articulationpoints_endpoints.write(
        G=sample_graph,
        write_property="articulationPoint",
    )

    assert isinstance(result, ArticulationPointsWriteResult)
    assert result.articulation_point_count >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.node_properties_written >= 0
