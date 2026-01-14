from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.centrality.bridges_cypher_endpoints import BridgesCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node),
    (b: Node),
    (c: Node),
    (a)-[:REL]->(b),
    (b)-[:REL]->(c)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {
            relationshipProperties: {},
            relationshipType: 'REL'
        }, {
            undirectedRelationshipTypes: ['REL']
        }) AS G
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
def bridges_endpoints(query_runner: QueryRunner) -> Generator[BridgesCypherEndpoints, None, None]:
    yield BridgesCypherEndpoints(query_runner)


def test_bridges_stream(bridges_endpoints: BridgesCypherEndpoints, sample_graph: GraphV2) -> None:
    """Test Bridges stream operation."""
    result_df = bridges_endpoints.stream(
        G=sample_graph,
    )

    assert set(result_df.columns) == {"from", "to", "remainingSizes"}
    # In a line graph a-b-c, both a-b and b-c are bridges
    assert len(result_df) == 2


def test_bridges_estimate(bridges_endpoints: BridgesCypherEndpoints, sample_graph: GraphV2) -> None:
    result = bridges_endpoints.estimate(sample_graph)

    assert result.node_count == 3
    assert result.relationship_count == 4
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
