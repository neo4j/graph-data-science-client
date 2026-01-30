from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.pathfinding.steiner_tree_cypher_endpoints import (
    SteinerTreeCypherEndpoints,
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
def steiner_tree_endpoints(query_runner: QueryRunner) -> Generator[SteinerTreeCypherEndpoints, None, None]:
    yield SteinerTreeCypherEndpoints(query_runner)


def test_steiner_tree_stream(
    steiner_tree_endpoints: SteinerTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")
    targets = [find_node_by_name(query_runner, "D"), find_node_by_name(query_runner, "E")]

    result_df = steiner_tree_endpoints.stream(
        G=sample_graph,
        source_node=source,
        target_nodes=targets,
        relationship_weight_property="cost",
    )

    assert set(result_df.columns) == {"nodeId", "parentId", "weight"}
    assert len(result_df) == 5


def test_steiner_tree_stats(
    steiner_tree_endpoints: SteinerTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")
    targets = [find_node_by_name(query_runner, "D"), find_node_by_name(query_runner, "E")]

    result = steiner_tree_endpoints.stats(
        G=sample_graph,
        source_node=source,
        target_nodes=targets,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


def test_steiner_tree_mutate(
    steiner_tree_endpoints: SteinerTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")
    targets = [find_node_by_name(query_runner, "D"), find_node_by_name(query_runner, "E")]

    result = steiner_tree_endpoints.mutate(
        G=sample_graph,
        mutate_relationship_type="STEINER_TREE",
        mutate_property="weight",
        source_node=source,
        target_nodes=targets,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 4
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


def test_steiner_tree_write(
    steiner_tree_endpoints: SteinerTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")
    targets = [find_node_by_name(query_runner, "D"), find_node_by_name(query_runner, "E")]

    result = steiner_tree_endpoints.write(
        G=sample_graph,
        write_relationship_type="STEINER_TREE",
        write_property="weight",
        source_node=source,
        target_nodes=targets,
        relationship_weight_property="cost",
    )

    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 4
    assert result.effective_node_count == 5
    assert result.effective_target_nodes_count == 2
    assert result.total_weight == 4
    assert "sourceNode" in result.configuration


def test_steiner_tree_estimate(
    steiner_tree_endpoints: SteinerTreeCypherEndpoints, sample_graph: GraphV2, query_runner: QueryRunner
) -> None:
    source = find_node_by_name(query_runner, "A")
    targets = [find_node_by_name(query_runner, "D"), find_node_by_name(query_runner, "E")]

    result = steiner_tree_endpoints.estimate(
        sample_graph,
        source_node=source,
        target_nodes=targets,
        relationship_weight_property="cost",
    )

    assert result.node_count == 6
    assert result.relationship_count == 6
    assert "Bytes" in result.required_memory
    assert result.bytes_min > 0
    assert result.bytes_max > 0
    assert result.heap_percentage_min > 0
    assert result.heap_percentage_max > 0
