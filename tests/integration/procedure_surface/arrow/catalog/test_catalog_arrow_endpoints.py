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
from graphdatascience.procedure_surface.arrow.catalog.catalog_arrow_endpoints import (
    CatalogArrowEndpoints,
)
from tests.integration.procedure_surface.arrow.graph_creation_helper import create_graph


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
    assert result.memory_usage
    assert result.size_in_bytes > 2000
    assert result.modification_time < datetime.now(timezone.utc)
    assert "p50" in result.degree_distribution  # type: ignore


def test_list_without_graph(
    catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph, arrow_client: AuthenticatedArrowClient
) -> None:
    with create_graph(arrow_client, "second_graph", "()") as g2:
        result = catalog_endpoints.list()

    assert len(result) == 2
    assert set(g.graph_name for g in result) == {sample_graph.name(), g2.name()}


def test_exists(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    assert catalog_endpoints.exists(sample_graph.name())
    assert not catalog_endpoints.exists("nonexistent")


def test_drop(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    res = catalog_endpoints.drop(sample_graph)

    assert res is not None
    assert res.graph_name == sample_graph.name()
    assert len(catalog_endpoints.list()) == 0


def test_drop_nonexistent(catalog_endpoints: CatalogArrowEndpoints) -> None:
    with pytest.raises(ArrowKeyError, match="does not exist on database"):
        catalog_endpoints.drop("nonexistent", fail_if_missing=True)


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


def test_construct_overwrite(arrow_client: AuthenticatedArrowClient) -> None:
    endpoints = CatalogArrowEndpoints(arrow_client)
    try:
        nodes1 = DataFrame({"nodeId": [0, 1], "labels": [["A"], ["A"]]})
        G1 = endpoints.construct(graph_name="g", nodes=nodes1, relationships=[])
        assert G1.node_count() == 2

        nodes2 = DataFrame({"nodeId": [0, 1, 2], "labels": [["A"], ["A"], ["A"]]})
        G2 = endpoints.construct(graph_name="g", nodes=nodes2, relationships=[], overwrite=True)
        assert G2.node_count() == 3
    finally:
        endpoints.drop("g", fail_if_missing=False)


def test_graph_generate_overwrite(catalog_endpoints: CatalogArrowEndpoints) -> None:
    try:
        catalog_endpoints.generate(
            "generated",
            node_count=10,
            average_degree=4,
            relationship_seed=42,
            sudo=True,
            log_progress=False,
            username="neo4j",
        )

        G, result = catalog_endpoints.generate(
            "generated",
            node_count=20,
            average_degree=4,
            relationship_seed=42,
            sudo=True,
            log_progress=False,
            username="neo4j",
            overwrite=True,
        )

        with G:
            assert result.name == "generated"
            assert result.nodes == 20
    finally:
        catalog_endpoints.drop("generated", fail_if_missing=False)


def test_graph_generate_async_overwrite(catalog_endpoints: CatalogArrowEndpoints) -> None:
    try:
        catalog_endpoints.generate_async(
            "generated",
            node_count=10,
            average_degree=4,
            relationship_seed=42,
            sudo=True,
            log_progress=False,
            username="neo4j",
        ).wait()

        handle = catalog_endpoints.generate_async(
            "generated",
            node_count=20,
            average_degree=4,
            relationship_seed=42,
            sudo=True,
            log_progress=False,
            username="neo4j",
            overwrite=True,
        )
        G, result = handle.result()

        with G:
            assert result["name"] == "generated"
            assert result["nodes"] == 20
    finally:
        catalog_endpoints.drop("generated", fail_if_missing=False)


def test_graph_filter_overwrite(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    try:
        _, r1 = catalog_endpoints.filter(
            sample_graph, graph_name="filtered", node_filter="n:A", relationship_filter="*"
        )
        assert r1.node_count == 2

        _, r2 = catalog_endpoints.filter(
            sample_graph,
            graph_name="filtered",
            node_filter="n:Node",
            relationship_filter="*",
            overwrite=True,
        )
        assert r2.graph_name == "filtered"
        assert r2.node_count == 3
    finally:
        catalog_endpoints.drop("filtered", fail_if_missing=False)


def test_graph_filter_async_overwrite(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    try:
        catalog_endpoints.filter_async(
            sample_graph, graph_name="filtered", node_filter="n:A", relationship_filter="*"
        ).wait()

        handle = catalog_endpoints.filter_async(
            sample_graph,
            graph_name="filtered",
            node_filter="n:Node",
            relationship_filter="*",
            overwrite=True,
        )
        G, result = handle.result()

        assert result["graphName"] == "filtered"
        assert result["nodeCount"] == 3
    finally:
        catalog_endpoints.drop("filtered", fail_if_missing=False)
