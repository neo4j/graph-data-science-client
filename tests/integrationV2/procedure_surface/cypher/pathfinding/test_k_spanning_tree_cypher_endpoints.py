from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.k_spanning_tree_cypher_endpoints import (
    KSpanningTreeCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'A'}),
    (b: Node {name: 'B'}),
    (c: Node {name: 'C'}),
    (d: Node {name: 'D'}),
    (e: Node {name: 'E'}),
    (f: Node {name: 'F'}),
    (a)-[:LINK {cost: 1.0}]->(b),
    (a)-[:LINK {cost: 1.0}]->(c),
    (b)-[:LINK {cost: 1.0}]->(d),
    (c)-[:LINK {cost: 1.0}]->(e),
    (d)-[:LINK {cost: 1.0}]->(f),
    (e)-[:LINK {cost: 1.0}]->(f)
    """

    projection_query = """
        MATCH (source)-[r]->(target)
        WITH gds.graph.project('g', source, target, {
            relationshipProperties: properties(r)
        }, {undirectedRelationshipTypes: ['*']}) AS G
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
def k_spanning_tree_endpoints(query_runner: QueryRunner) -> Generator[KSpanningTreeCypherEndpoints, None, None]:
    yield KSpanningTreeCypherEndpoints(query_runner)


def test_k_spanning_tree_write(
    k_spanning_tree_endpoints: KSpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result = k_spanning_tree_endpoints.write(
        G=sample_graph,
        k=3,
        write_property="weight",
        source_node=source,
        relationship_weight_property="cost",
    )

    assert result.effective_node_count == 3
    assert result.write_millis >= 0
    assert result.compute_millis >= 0
    assert result.pre_processing_millis >= 0
    assert result.post_processing_millis >= 0
