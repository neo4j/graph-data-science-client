from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.cypher.graph_sampling_cypher_endpoints import GraphSamplingCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import delete_all_graphs


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
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

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {relationshipProperties: {weight: r.weight}}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    delete_all_graphs(query_runner)
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def graph_sampling_endpoints(query_runner: QueryRunner) -> Generator[GraphSamplingCypherEndpoints, None, None]:
    yield GraphSamplingCypherEndpoints(query_runner)


def test_rwr_basic(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test RWR sampling with basic configuration."""
    result = graph_sampling_endpoints.rwr(
        G=sample_graph, graph_name="rwr_sampled", start_nodes=[0, 1], restart_probability=0.15, sampling_ratio=0.8
    )

    assert result.graph_name == "rwr_sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.relationship_count >= 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_rwr_with_weights(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test RWR sampling with weighted relationships."""
    result = graph_sampling_endpoints.rwr(
        G=sample_graph,
        graph_name="rwr_weighted",
        restart_probability=0.2,
        sampling_ratio=0.6,
        relationship_weight_property="weight",
    )

    assert result.graph_name == "rwr_weighted"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_rwr_minimal_config(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test RWR sampling with minimal configuration."""
    result = graph_sampling_endpoints.rwr(G=sample_graph, graph_name="rwr_minimal")

    assert result.graph_name == "rwr_minimal"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.project_millis >= 0


def test_cnarw_basic(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test CNARW sampling with basic configuration."""
    result = graph_sampling_endpoints.cnarw(
        G=sample_graph, graph_name="cnarw_sampled", restart_probability=0.15, sampling_ratio=0.8
    )

    assert result.graph_name == "cnarw_sampled"
    assert result.from_graph_name == sample_graph.name()
    assert result.node_count > 0
    assert result.relationship_count >= 0
    assert result.start_node_count >= 1
    assert result.project_millis >= 0


def test_cnarw_with_stratification(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test CNARW sampling with node label stratification."""
    result = graph_sampling_endpoints.cnarw(
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


def test_cnarw_minimal_config(graph_sampling_endpoints: GraphSamplingCypherEndpoints, sample_graph: Graph) -> None:
    """Test CNARW sampling with minimal configuration."""
    result = graph_sampling_endpoints.cnarw(G=sample_graph, graph_name="cnarw_minimal")

    assert result.graph_name == "cnarw_minimal"
    assert result.from_graph_name == sample_graph.name()
    assert result.start_node_count >= 1
    assert result.project_millis >= 0
