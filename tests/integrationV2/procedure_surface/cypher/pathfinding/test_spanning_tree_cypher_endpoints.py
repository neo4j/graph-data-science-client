from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.spanning_tree_cypher_endpoints import (
    SpanningTreeCypherEndpoints,
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
def spanning_tree_endpoints(query_runner: QueryRunner) -> Generator[SpanningTreeCypherEndpoints, None, None]:
    yield SpanningTreeCypherEndpoints(query_runner)


def test_spanning_tree_stream(
    spanning_tree_endpoints: SpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result_df = spanning_tree_endpoints.stream(
        G=sample_graph,
        source_node=source,
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert len(result_df) == 6


def test_spanning_tree_stats(
    spanning_tree_endpoints: SpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result = spanning_tree_endpoints.stats(
        G=sample_graph,
        source_node=source,
        relationship_weight_property="cost",
    )

    assert result.total_weight == 5.0
    assert result.effective_node_count == 6
    assert result.compute_millis >= 0


def test_spanning_tree_mutate(
    spanning_tree_endpoints: SpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result = spanning_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="TREE",
        mutate_property="weight",
        source_node=source,
        relationship_weight_property="cost",
    )

    assert result.total_weight == 5.0
    assert result.effective_node_count == 6
    assert result.relationships_written == 5
    assert result.mutate_millis >= 0


def test_spanning_tree_write(
    spanning_tree_endpoints: SpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result = spanning_tree_endpoints.write(
        G=sample_graph,
        write_relationship_type="TREE",
        write_property="weight",
        source_node=source,
        relationship_weight_property="cost",
    )

    assert result.total_weight == 5.0
    assert result.effective_node_count == 6
    assert result.relationships_written == 5
    assert result.write_millis >= 0


def test_spanning_tree_estimate(
    spanning_tree_endpoints: SpanningTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")

    result = spanning_tree_endpoints.estimate(
        G=sample_graph,
        source_node=source,
        relationship_weight_property="cost",
    )

    assert result.bytes_min > 0
    assert result.bytes_max > 0
