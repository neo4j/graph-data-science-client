import datetime
from typing import Generator

import pytest
from pyarrow import ArrowKeyError
from pyarrow._flight import FlightServerError

from graphdatascience import Graph, QueryRunner
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.catalog_endpoints import RelationshipPropertySpec
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


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
    assert "nodes" in result.graph_schema
    assert "nodes" in result.schema_with_orientation
    assert result.creation_time < datetime.datetime.now(datetime.timezone.utc)
    assert result.database == "neo4j"
    assert result.database_location == "local"
    assert "KiB" in result.memory_usage
    assert result.size_in_bytes > 20000
    assert result.modification_time < datetime.datetime.now(datetime.timezone.utc)
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
        result = endpoints.project(
            graph_name="g",
            query="UNWIND range(1, 10) AS x WITH gds.graph.project.remote(x, null) as g RETURN g",
        )

        assert result.graph_name == "g"
        assert result.node_count == 10
        assert result.relationship_count == 0
        assert result.project_millis >= 0

        assert len(endpoints.list("g")) == 1
    finally:
        endpoints.drop("g", fail_if_missing=False)


def test_graph_filter(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    try:
        result = catalog_endpoints.filter(
            sample_graph, graph_name="filtered", node_filter="n:A", relationship_filter="*"
        )

        assert result.node_count == 2
        assert result.relationship_count == 0
        assert result.from_graph_name == sample_graph.name()
        assert result.graph_name == "filtered"
        assert result.project_millis >= 0
    finally:
        try:
            catalog_endpoints.drop("filtered", fail_if_missing=False)
        except FlightServerError:
            # There is currently a bug in GDS that throws when deleting a GDL graph
            pass


def test_graph_generate(catalog_endpoints: CatalogArrowEndpoints) -> None:
    try:
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

    finally:
        try:
            catalog_endpoints.drop("generated", fail_if_missing=False)
        except FlightServerError:
            # There is currently a bug in GDS that throws when deleting a GDL graph
            pass
