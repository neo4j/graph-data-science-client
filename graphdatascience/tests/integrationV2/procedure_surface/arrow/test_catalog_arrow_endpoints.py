import datetime
import json
from datetime import tzinfo
from typing import Generator

import pytest

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog_arrow_endpoints import CatalogArrowEndpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import create_graph


@pytest.fixture
def sample_graph(arrow_client: AuthenticatedArrowClient) -> Generator[Graph, None, None]:
    gdl = """
    (a: Node)
    (b: Node)
    (c: Node)
    (a)-[:REL]->(c)
    """

    yield create_graph(arrow_client, "g", gdl)
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "g"}).encode("utf-8"))


@pytest.fixture
def catalog_endpoints(arrow_client: AuthenticatedArrowClient) -> Generator[CatalogArrowEndpoints, None, None]:
    yield CatalogArrowEndpoints(arrow_client)


def test_list_with_graph(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph) -> None:
    """Test listing graphs with a specific graph."""
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
    assert "p50" in result.degree_distribution

def test_list_without_graph(catalog_endpoints: CatalogArrowEndpoints, sample_graph: Graph, arrow_client: AuthenticatedArrowClient) -> None:
    g2 = create_graph(arrow_client, "second_graph", "()")
    result = catalog_endpoints.list()
    arrow_client.do_action("v2/graph.drop", json.dumps({"graphName": "g2"}).encode("utf-8"))

    assert len(result) == 2
    assert set(g.graph_name for g in result) == {sample_graph.name(), g2.name()}


