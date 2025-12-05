from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.prize_steiner_tree_cypher_endpoints import (
    PrizeSteinerTreeCypherEndpoints,
)
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {name: 'A', prize: 5.0}),
    (b: Node {name: 'B', prize: 10.0}),
    (c: Node {name: 'C', prize: 15.0}),
    (d: Node {name: 'D', prize: 8.0}),
    (e: Node {name: 'E', prize: 12.0}),
    (a)-[:LINK {cost: 1.0}]->(b),
    (b)-[:LINK {cost: 2.0}]->(c),
    (a)-[:LINK {cost: 3.0}]->(d),
    (d)-[:LINK {cost: 1.5}]->(e),
    (e)-[:LINK {cost: 2.5}]->(c)
    """

    projection_query = """
        MATCH (source)-[r]->(target)
        WITH gds.graph.project('g', source, target, {
            sourceNodeProperties: {prize: source.prize},
            targetNodeProperties: {prize: target.prize},
            relationshipProperties: {cost: r.cost}
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
def prize_steiner_tree_endpoints(query_runner: QueryRunner) -> Generator[PrizeSteinerTreeCypherEndpoints, None, None]:
    yield PrizeSteinerTreeCypherEndpoints(query_runner)


def test_prize_steiner_tree_stream(
    prize_steiner_tree_endpoints: PrizeSteinerTreeCypherEndpoints, sample_graph: GraphV2
) -> None:
    result_df = prize_steiner_tree_endpoints.stream(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert len(result_df) == 4


def test_prize_steiner_tree_stats(
    prize_steiner_tree_endpoints: PrizeSteinerTreeCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.stats(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.compute_millis >= 0


def test_prize_steiner_tree_mutate(
    prize_steiner_tree_endpoints: PrizeSteinerTreeCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="TREE",
        mutate_property="weight",
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.relationships_written == 4
    assert result.mutate_millis >= 0


def test_prize_steiner_tree_write(
    prize_steiner_tree_endpoints: PrizeSteinerTreeCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.write(
        G=sample_graph,
        write_relationship_type="TREE",
        write_property="weight",
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.total_weight == 7.0
    assert result.sum_of_prizes == 50.0
    assert result.effective_node_count == 5
    assert result.relationships_written == 4
    assert result.write_millis >= 0


def test_prize_steiner_tree_estimate(
    prize_steiner_tree_endpoints: PrizeSteinerTreeCypherEndpoints, sample_graph: GraphV2
) -> None:
    result = prize_steiner_tree_endpoints.estimate(
        G=sample_graph,
        prize_property="prize",
        relationship_weight_property="cost",
    )

    assert result.bytes_min > 0
    assert result.bytes_max > 0
