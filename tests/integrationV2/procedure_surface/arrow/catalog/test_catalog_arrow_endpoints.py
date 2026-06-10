from datetime import datetime, timezone
from typing import Generator

import pytest
from pandas import DataFrame
from pyarrow import ArrowKeyError

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.graph.graph_api import Graph
from graphdatascience.procedure_surface.api.catalog.catalog_endpoints import (
    RelationshipPropertySpec,
)
from graphdatascience.procedure_surface.arrow.catalog import ProjectionResult
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import (
    CatalogArrowEndpoints,
)
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType
from tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a :Node:A)
    (b :Node:A)
    (c :Node:B)
    (a)-[:REL]->(c)
    """

    with create_graph(arrow_client, "g", gdl) as G:
        yield G


@pytest.fixture
def catalog_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


def test_list_with_graph(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    results = catalog_endpoints.list(G=sample_graph)

    assert len(results) == 1
    result = results[0]

    assert result.graph_name == "g"
    assert result.node_count == 3
    assert result.relationship_count == 1
    assert result.graph_schema == {
        "nodes": {"A": {}, "B": {}, "Node": {}},
        "relationships": {"REL": {"direction": "DIRECTED", "properties": {}}},
    }

    assert result.creation_time < datetime.now(timezone.utc)
    assert result.database == "neo4j"
    assert result.database_location == "local"
    assert result.memory_usage and result.memory_usage.endswith("KiB")
    assert result.size_in_bytes > 20000
    assert result.modification_time < datetime.now(timezone.utc)
    assert "p50" in result.degree_distribution  # type: ignore


def test_list_without_graph(
    catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph, arrow_client: AuthenticatedArrowClient
) -> None:
    with create_graph(arrow_client, "second_graph", "()") as g2:
        result = catalog_endpoints.list()

    assert len(result) == 2
    assert set(g.graph_name for g in result) == {sample_graph.name(), g2.name()}


def test_drop(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    res = catalog_endpoints.drop(sample_graph)

    assert res is not None
    assert res.graph_name == sample_graph.name()
    assert len(catalog_endpoints.list()) == 0


def test_drop_nonexistent(catalog_endpoints: CatalogArrowEndpoints) -> None:
    with pytest.raises(ArrowKeyError, match="does not exist on database"):
        catalog_endpoints.drop("nonexistent", fail_if_missing=True)


@pytest.mark.db_integration
def test_projection(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> None:
    try:
        endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
        G, result = endpoints.project(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
        )

        assert isinstance(result, ProjectionResult)

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_projection_with_query_parameters(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> None:
    try:
        endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
        G, result = endpoints.project(
            graph_name="g",
            query="UNWIND range(1, $LIMIT) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
            query_parameters={"LIMIT": 10},
        )

        assert isinstance(result, ProjectionResult)

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_store_projection(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> None:
    try:
        query_runner.run_cypher(
            "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
            QueryType.USER_ACTION,
        )

        endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
        G, result = endpoints.project_native(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )

        assert G.name() == "g"
        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 5

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


def test_construct(arrow_client: AuthenticatedArrowClient) -> None:
    nodes = DataFrame(
        {
            "nodeId": [0, 1],
            "labels": [["A"], ["B"]],
            "propA": [1337, 42.1],
        }
    )
    relationships = DataFrame(
        {
            "sourceNodeId": [0, 1],
            "targetNodeId": [1, 0],
            "relationshipType": ["REL", "REL2"],
            "relPropA": [1337.2, 42],
        }
    )

    endpoints = CatalogArrowEndpoints(arrow_client)
    with endpoints.construct(
        graph_name="g",
        nodes=nodes,
        relationships=relationships,
    ) as G:
        assert G.name() == "g"
        assert G.node_count() == 2
        assert G.relationship_count() == 2

        assert len(endpoints.list("g")) == 1


def test_load_dataset(catalog_endpoints: CatalogArrowEndpoints) -> None:
    with catalog_endpoints.datasets.load_karate_club() as G:
        assert G.name() == "karate_club"
        assert G.node_count() == 34
        assert G.relationship_count() == 78


def test_graph_filter(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    try:
        G, result = catalog_endpoints.filter(
            sample_graph, graph_name="filtered", node_filter="n:A", relationship_filter="*"
        )

        assert G.name() == "filtered"
        assert result.node_count == 2
        assert result.relationship_count == 0
        assert result.from_graph_name == sample_graph.name()
        assert result.graph_name == "filtered"
        assert result.project_millis >= 0
    finally:
        catalog_endpoints.drop("filtered", fail_if_missing=False)


def test_graph_generate_with_relationships_property(catalog_endpoints: CatalogArrowEndpoints) -> None:
    G, result = catalog_endpoints.generate(
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

    with G:
        assert G.name() == "generated"
        assert result.name == "generated"
        assert result.nodes == 10
        assert result.relationships > 5
        assert result.generate_millis >= 0
        assert result.relationship_distribution == "UNIFORM"
        assert result.relationship_property == RelationshipPropertySpec.fixed("weight", 42)

        assert catalog_endpoints.list("generated") is not None


@pytest.mark.db_integration
def test_projection_async(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> None:
    try:
        endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
        handle = endpoints.project_async(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
        )

        assert handle.job_id()

        handle.wait()
        assert handle.done()

        G, result = handle.result()

        assert G.name() == "g"
        assert result["graphName"] == "g"
        assert result["nodeCount"] == 10
        assert result["relationshipCount"] == 0
        assert result["projectMillis"] >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


@pytest.mark.db_integration
def test_store_projection_async(arrow_client: AuthenticatedArrowClient, query_runner: QueryRunner) -> None:
    try:
        query_runner.run_cypher(
            "UNWIND range(1, 5) AS x CREATE (:Person)-[:KNOWS]->(:Person)",
            QueryType.USER_ACTION,
        )

        endpoints = CatalogArrowEndpoints(arrow_client, query_runner)
        handle = endpoints.project_native_async(
            graph_name="g",
            node_label_filter=["Person"],
            relationship_type_filter=["KNOWS"],
        )

        assert handle.job_id()

        G, result = handle.result()

        assert G.name() == "g"
        assert result["graphName"] == "g"
        assert result["nodeCount"] == 10
        assert result["relationshipCount"] == 5

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)
        query_runner.run_cypher("MATCH (n) DETACH DELETE n", QueryType.USER_ACTION)


def test_graph_filter_async(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    try:
        handle = catalog_endpoints.filter_async(
            sample_graph,
            graph_name="filtered",
            node_filter="n:A",
            relationship_filter="*",
        )

        assert handle.job_id()

        G, result = handle.result()

        assert G.name() == "filtered"
        assert result["nodeCount"] == 2
        assert result["relationshipCount"] == 0
        assert result["fromGraphName"] == sample_graph.name()
        assert result["graphName"] == "filtered"
        assert result["projectMillis"] >= 0
    finally:
        catalog_endpoints.drop("filtered", fail_if_missing=False)


def test_graph_generate_async(catalog_endpoints: CatalogArrowEndpoints) -> None:
    handle = catalog_endpoints.generate_async(
        "generated",
        node_count=10,
        average_degree=5,
        relationship_distribution="UNIFORM",
        relationship_seed=42,
        orientation="UNDIRECTED",
        allow_self_loops=False,
        read_concurrency=1,
        sudo=True,
        log_progress=False,
        username="neo4j",
    )

    assert handle.job_id()

    G, result = handle.result()

    with G:
        assert G.name() == "generated"
        assert result["name"] == "generated"
        assert result["nodes"] == 10
        assert result["relationships"] > 5
        assert result["generateMillis"] >= 0
        assert result["relationshipDistribution"] == "UNIFORM"
        assert catalog_endpoints.list("generated") is not None


def test_graph_generate(catalog_endpoints: CatalogArrowEndpoints) -> None:
    G, result = catalog_endpoints.generate(
        "generated",
        node_count=10,
        average_degree=5,
        relationship_distribution="UNIFORM",
        relationship_seed=42,
        orientation="UNDIRECTED",
        allow_self_loops=False,
        read_concurrency=1,
        sudo=True,
        log_progress=False,
        username="neo4j",
    )

    with G:
        assert G.name() == "generated"
        assert result.name == "generated"
        assert result.nodes == 10
        assert result.relationships > 5
        assert result.generate_millis >= 0
        assert result.relationship_distribution == "UNIFORM"
        assert result.relationship_property is None
        assert catalog_endpoints.list("generated") is not None
