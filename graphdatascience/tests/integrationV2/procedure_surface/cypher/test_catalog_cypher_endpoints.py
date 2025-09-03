import datetime
from typing import Generator

import pytest

from graphdatascience import Graph, QueryRunner
from graphdatascience.procedure_surface.api.catalog_endpoints import RelationshipPropertySpec
from graphdatascience.procedure_surface.cypher.catalog_cypher_endpoints import CatalogCypherEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.cypher.cypher_graph_helper import delete_all_graphs


@pytest.fixture
def sample_graph(query_runner: QueryRunner) -> Generator[Graph, None, None]:
    create_statement = """
    CREATE
    (a: Node:A {id: 0}),
    (b: Node:A {id: 1}),
    (c: Node:B {id: 2}),
    (a)-[:REL]->(c)
    """

    query_runner.run_cypher(create_statement)

    query_runner.run_cypher("""
        MATCH (n)
        OPTIONAL MATCH (n)-[r]->(m)
        WITH gds.graph.project('g', n, m, {sourceNodeLabels: labels(n), targetNodeLabels: labels(m)}) AS G
        RETURN G
    """)

    yield Graph("g", query_runner)

    delete_all_graphs(query_runner)
    query_runner.run_cypher("MATCH (n) DETACH DELETE n")


@pytest.fixture
def catalog_endpoints(query_runner: QueryRunner) -> Generator[CatalogCypherEndpoints, None, None]:
    yield CatalogCypherEndpoints(query_runner)


def test_list_with_graph(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    results = catalog_endpoints.list(G=sample_graph)

    assert len(results) == 1
    result = results[0]

    assert result.graph_name == "g"
    assert result.node_count == 3
    assert result.relationship_count == 1
    assert "nodes" in result.graph_schema
    assert "nodes" in result.schema_with_orientation
    assert result.creation_time < datetime.datetime.now(datetime.timezone.utc)
    assert result.database == "neo4j"
    assert result.database_location == "local"
    assert "KiB" in result.memory_usage
    assert result.size_in_bytes > 0
    assert result.modification_time < datetime.datetime.now(datetime.timezone.utc)


def test_list_with_graph_name_string(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    results = catalog_endpoints.list(G="g")

    assert len(results) == 1
    result = results[0]
    assert result.graph_name == "g"
    assert result.node_count == 3
    assert result.relationship_count == 1


def test_list_without_graph(
    catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph, query_runner: QueryRunner
) -> None:
    try:
        query_runner.run_cypher("CREATE (x:Test)")
        query_runner.run_cypher("""
            MATCH (n:Test)
            WITH gds.graph.project('second_graph', n, null) AS G
            RETURN G
        """)

        g2 = Graph("second_graph", query_runner)
        result = catalog_endpoints.list()

        assert len(result) == 2
        assert set(g.graph_name for g in result) == {sample_graph.name(), g2.name()}
    finally:
        query_runner.run_cypher("MATCH (n:Test) DELETE n")


def test_drop_with_graph_object(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    res = catalog_endpoints.drop(sample_graph)

    assert res is not None
    assert res.graph_name == sample_graph.name()
    assert len(catalog_endpoints.list()) == 0


def test_drop_with_graph_name_string(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    res = catalog_endpoints.drop("g")

    assert res is not None
    assert res.graph_name == "g"
    assert len(catalog_endpoints.list()) == 0


def test_drop_nonexistent_fail_if_missing_true(catalog_endpoints: CatalogCypherEndpoints) -> None:
    with pytest.raises(Exception):  # Should raise some kind of exception
        catalog_endpoints.drop("nonexistent", fail_if_missing=True)


def test_drop_nonexistent_fail_if_missing_false(catalog_endpoints: CatalogCypherEndpoints) -> None:
    res = catalog_endpoints.drop("nonexistent", fail_if_missing=False)
    assert res is None


def test_graph_filter(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    result = catalog_endpoints.filter(
        sample_graph, graph_name="filtered", node_filter="n:A", relationship_filter="FALSE"
    )

    assert result.node_count == 2
    assert result.relationship_count == 0
    assert result.from_graph_name == sample_graph.name()
    assert result.graph_name == "filtered"
    assert result.node_filter == "n:A"
    assert result.relationship_filter == "FALSE"
    assert result.project_millis >= 0


def test_sample_property(catalog_endpoints: CatalogCypherEndpoints) -> None:
    """Test that the sample property returns GraphSamplingCypherEndpoints."""
    sample_endpoints = catalog_endpoints.sample

    # Check that it's the correct type
    from graphdatascience.procedure_surface.cypher.graph_sampling_cypher_endpoints import GraphSamplingCypherEndpoints

    assert isinstance(sample_endpoints, GraphSamplingCypherEndpoints)


def test_projection(catalog_endpoints: CatalogCypherEndpoints, sample_graph: Graph) -> None:
    G, result = catalog_endpoints.project("g2", ["A", "B"], "REL", node_properties=["id"], read_concurrency=2)

    assert result.node_count == 3
    assert result.relationship_count == 1
    assert result.graph_name == "g2"

    assert catalog_endpoints.list(G)[0].graph_name == "g2"


def test_graph_generate(catalog_endpoints: CatalogCypherEndpoints) -> None:
    result = catalog_endpoints.generate(
        "generated",
        node_count=10,
        average_degree=5,
        relationship_distribution="UNIFORM",
        relationship_seed=42,
        relationship_property=RelationshipPropertySpec.fixed("weight", 42),
        orientation="UNDIRECTED",
        allow_self_loops=False,
        read_concurrency=1,
        sudo=True,
        log_progress=False,
        username="neo4j",
    )

    assert result.name == "generated"
    assert result.nodes == 10
    assert result.relationships > 5
    assert result.generate_millis >= 0
    assert result.relationship_distribution == "UNIFORM"
    assert result.relationship_property == RelationshipPropertySpec.fixed("weight", 42)

    assert catalog_endpoints.list("generated") is not None
