from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.procedure_surface.api.catalog.graph_api import GraphV2
from graphdatascience.procedure_surface.api.pathfinding.max_flow_endpoints import (
    MaxFlowMutateResult,
    MaxFlowStatsResult,
    MaxFlowWriteResult,
)
from graphdatascience.procedure_surface.cypher.pathfinding.max_flow_cypher_endpoints import MaxFlowCypherEndpoints
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import create_graph
from tests.integrationV2.procedure_surface.node_lookup_helper import find_node_by_name


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
        CREATE
              (a:Node {name: 'A'})
            , (b:Node {name: 'B'})
            , (c:Node {name: 'C'})
            , (d:Node {name: 'D'})
            , (e:Node {name: 'E'})
            , (f:Node {name: 'F'})
            , (a)-[:TYPE {capacity: 3.0}]->(b)
            , (a)-[:TYPE {capacity: 1.0}]->(c)
            , (b)-[:TYPE {capacity: 4.0}]->(d)
            , (c)-[:TYPE {capacity: 2.0}]->(d)
            , (b)-[:TYPE {capacity: 2.0}]->(e)
            , (d)-[:TYPE {capacity: 1.0}]->(e)
            , (d)-[:TYPE {capacity: 3.0}]->(f)
            , (e)-[:TYPE {capacity: 2.0}]->(f)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipProperties: properties(r)}) AS G
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
def maxflow_endpoints(query_runner: QueryRunner) -> MaxFlowCypherEndpoints:
    return MaxFlowCypherEndpoints(query_runner)


def test_maxflow_stats(maxflow_endpoints: MaxFlowCypherEndpoints, sample_graph: GraphV2) -> None:
    result = maxflow_endpoints.stats(
        sample_graph,
        source_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "F")],
        capacity_property="capacity",
    )

    assert isinstance(result, MaxFlowStatsResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0


def test_maxflow_stream(maxflow_endpoints: MaxFlowCypherEndpoints, sample_graph: GraphV2) -> None:
    result = maxflow_endpoints.stream(
        sample_graph,
        capacity_property="capacity",
        source_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "F")],
    )

    assert set(result.columns) == {"source", "target", "flow"}
    assert len(result) == 7


def test_maxflow_mutate(maxflow_endpoints: MaxFlowCypherEndpoints, sample_graph: GraphV2) -> None:
    result = maxflow_endpoints.mutate(
        sample_graph,
        mutate_property="flow",
        source_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "F")],
        mutate_relationship_type="FLOW",
        capacity_property="capacity",
    )

    assert isinstance(result, MaxFlowMutateResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.mutate_millis >= 0
    assert result.relationships_written == 7


def test_maxflow_estimate(maxflow_endpoints: MaxFlowCypherEndpoints, sample_graph: GraphV2) -> None:
    result = maxflow_endpoints.estimate(
        G=sample_graph,
        source_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "F")],
        capacity_property="capacity",
    )

    assert result.node_count == 6
    assert result.relationship_count == 8
    assert result.required_memory is not None
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0


def test_maxflow_write(maxflow_endpoints: MaxFlowCypherEndpoints, sample_graph: GraphV2) -> None:
    result = maxflow_endpoints.write(
        sample_graph,
        write_property="flow",
        source_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "A")],
        target_nodes=[find_node_by_name(maxflow_endpoints._query_runner, "F")],
        write_relationship_type="FLOW",
        capacity_property="capacity",
    )

    assert isinstance(result, MaxFlowWriteResult)
    assert result.total_flow >= 0
    assert result.pre_processing_millis >= 0
    assert result.compute_millis >= 0
    assert result.write_millis >= 0
    assert result.relationships_written == 7
