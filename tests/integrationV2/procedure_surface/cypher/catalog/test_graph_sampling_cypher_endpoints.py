from typing import Generator

import pytest

from graphdatascience import QueryRunner
from graphdatascience.graph.v2.graph_api import GraphV2
from graphdatascience.procedure_surface.cypher.catalog.graph_sampling_cypher_endpoints import (
    GraphSamplingCypherEndpoints,
)
from tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import (
    create_graph,
)


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[GraphV2, None, None]:
    create_statement = """
    CREATE
    (a: Node {id: 0}),
    (b: Node {id: 1}),
    (c: Node {id: 2}),
    (d: Node {id: 3}),
    (e: Node {id: 4}),
    (a)-[:REL {weight: 1.0}]->(b),
    (b)-[:REL {weight: 2.0}]->(c),
    (c)-[:REL {weight: 1.5}]->(d),
    (d)-[:REL {weight: 0.5}]->(e),
    (e)-[:REL {weight: 1.2}]->(a)
    """

    projection_query = """
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipProperties: {weight: r.weight}}) AS G
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
def graph_sampling_endpoints(query_runner: QueryRunner) -> Generator[GraphSamplingCypherEndpoints, None, None]:
    yield GraphSamplingCypherEndpoints(query_runner)


def test_rwr_basic(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: GraphV2) -> None:
    G, result = graph_sampling_endpoints.rwr(
        G=sample_graph, graph_name="rwr_sampled", restart_probability=0.15, sampling_ratio=0.8
    )

    assert result.graph_name == "rwr_sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.relationship_count >= 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_cnarw_with_stratification(
    graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: GraphV2
) -> None:
    G, result = graph_sampling_endpoints.cnarw(
        G=sample_graph,
        graph_name="cnarw_stratified",
        restart_probability=0.1,
        sampling_ratio=0.7,
        node_label_stratification=True,
    )

    assert result.graph_name == "cnarw_stratified"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_cnarw_estimate(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: GraphV2) -> None:
    result = graph_sampling_endpoints.estimate(G=sample_graph, restart_probability=0.15, sampling_ratio=0.8)

    assert result.node_count == 5
    assert result.relationship_count == 5
    assert "Bytes" in result.required_memory
    assert result.tree_view is not None
    assert isinstance(result.map_view, dict)
    assert result.bytes_min >= 0
    assert result.bytes_max >= 0
    assert result.heap_percentage_min >= 0
    assert result.heap_percentage_max >= 0
