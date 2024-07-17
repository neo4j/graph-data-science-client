from graphdatascience.graph.graph_object import Graph
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.tests.unit.conftest import CollectingQueryRunner


def test_version(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    assert gds.version() == f"{gds._server_version}"


def test_list(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.list()

    assert runner.last_query() == "CALL gds.list()"
    assert runner.last_params() == {}


def test_as_node(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.util.asNode(1)

    assert runner.last_query() == "RETURN gds.util.asNode(1) AS node"


def test_remote_as_node(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    aura_gds.util.asNode(1)

    assert runner.last_query() == "MATCH (n) WHERE id(n) = $nodeId RETURN n"
    assert runner.last_params() == {"nodeId": 1}


def test_as_nodes(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.util.asNodes([1, 2, 3])

    assert runner.last_query() == "RETURN gds.util.asNodes([1, 2, 3]) AS nodes"


def test_remote_as_nodes(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    aura_gds.util.asNodes([1, 2, 3])

    assert runner.last_query() == "MATCH (n) WHERE id(n) IN $nodeIds RETURN collect(n)"
    assert runner.last_params() == {"nodeIds": [1, 2, 3]}


def test_node_property(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G = Graph("g", runner, gds._server_version)
    gds.util.nodeProperty(G, 1, "my_prop", "my_label")

    assert runner.last_query() == "RETURN gds.util.nodeProperty($graph_name, $node_id, $property_key, $node_label)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_id": 1,
        "property_key": "my_prop",
        "node_label": "my_label",
    }


def test_remote_node_property(runner: CollectingQueryRunner, aura_gds: AuraGraphDataScience) -> None:
    G = Graph("g", runner, aura_gds._server_version)
    aura_gds.util.nodeProperty(G, 1, "my_prop", "my_label")

    assert runner.last_query() == "RETURN gds.util.nodeProperty($graph_name, $node_id, $property_key, $node_label)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_id": 1,
        "property_key": "my_prop",
        "node_label": "my_label",
    }
