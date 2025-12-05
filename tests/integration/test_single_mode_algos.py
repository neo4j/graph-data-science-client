from typing import Generator

import pytest
from pytest import fixture

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion

GRAPH_NAME = "g"


@fixture(autouse=True)
def run_around_tests(gds: GraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    gds.run_cypher(
        """
        CREATE
        (a: Node),
        (b: Node),
        (c: Node),
        (a)-[:REL]->(b),
        (a)-[:REL]->(c),
        (b)-[:REL]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    gds.run_cypher("MATCH (n) DETACH DELETE n")
    gds.graph.drop(GRAPH_NAME)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_triangles(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"orientation": "UNDIRECTED"}})

    result = gds.triangles(G, maxDegree=2)

    assert len(result) == 1


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 5, 0))
def test_alpha_triangles(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"orientation": "UNDIRECTED"}})

    result = gds.alpha.triangles(G, maxDegree=2)

    assert len(result) == 1
