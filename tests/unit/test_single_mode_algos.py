import pytest

from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.unit.conftest import CollectingQueryRunner

GRAPH_NAME = "g"


@pytest.fixture
def G(gds: GraphDataScience) -> Graph:
    G_, _ = gds.graph.project(GRAPH_NAME, "Node", "REL")
    return G_


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_simple_mutate(runner: CollectingQueryRunner, gds: GraphDataScience, G: Graph) -> None:
    gds.triangles(G, maxDegree=2)
    assert runner.last_query() == "CALL gds.triangles($graph_name, $config)"
    jobId = runner.last_params().get("config", {}).get("jobId", "")
    assert runner.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"maxDegree": 2, "jobId": jobId},
    }
