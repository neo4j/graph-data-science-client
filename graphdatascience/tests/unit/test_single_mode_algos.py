import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GRAPH_NAME = "g"


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project(GRAPH_NAME, "Node", "REL")
    return G_


def test_simple_mutate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.alpha.triangles(G, maxDegree=2)

    assert runner.last_query() == "CALL gds.alpha.triangles($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"maxDegree": 2},
    }
