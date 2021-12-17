from gdsclient import GraphDataScience
from . import TestQueryRunner


RUNNER = TestQueryRunner()
gds = GraphDataScience(RUNNER)
GRAPH_NAME = "g"


def test_pageRank_mutate():
    graph = gds.graph.create(GRAPH_NAME, "Node", "REL")

    gds.pageRank.mutate(graph, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    assert RUNNER.last_query() == "CALL gds.pageRank.mutate($graph_name, $config)"
    assert RUNNER.last_params() == {
        "graph_name": GRAPH_NAME,
        "config": {"dampingFactor": 0.2, "tolerance": 0.3},
    }
