import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_all(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g")

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert (
        runner.last_query()
        == """MATCH (source)-->(target)
RETURN gds.graph.project($graph_name, source, target)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert (
        runner.last_query()
        == """MATCH (source)
OPTIONAL MATCH (source)-->(target)
RETURN gds.graph.project($graph_name, source, target)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_inverse_graph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", inverse=True)  # TODO: or using orientation="INVERSE"?

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert (
        runner.last_query()
        == """MATCH (source)<--(target)
RETURN gds.graph.project($graph_name, source, target)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_single_node_label(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A")

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g", data_config={"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"]}
    )

    assert (
        runner.last_query()
        == """MATCH (source:A)-->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_single_node_label(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g", data_config={"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"]}
    )

    assert (
        runner.last_query()
        == """MATCH (source:A)
OPTIONAL MATCH (source)-->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_single_node_label_alias(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=dict(Target="Label"))

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g", data_config={"sourceNodeLabels": ["Target"], "targetNodeLabels": ["Target"]}
    )

    assert (
        runner.last_query()
        == """MATCH (source:Label)-->(target:Label)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_multiple_node_labels_and(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="AND")

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g", data_config={"sourceNodeLabels": ["A", "B"], "targetNodeLabels": ["A", "B"]}
    )

    assert (
        runner.last_query()
        == """MATCH (source:A:B)-->(target:A:B)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_multiple_node_labels_and(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="AND", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g", data_config={"sourceNodeLabels": ["A", "B"], "targetNodeLabels": ["A", "B"]}
    )

    assert (
        runner.last_query()
        == """MATCH (source:A:B)
OPTIONAL MATCH (source)-->(target:A:B)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_multiple_node_labels_or(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="OR")

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert runner.last_query() == (
        """MATCH (source)-->(target)
WHERE (source:A OR source:B) AND (target:A OR target:B)
RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target)})"
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_multiple_node_labels_or(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="OR", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert runner.last_query() == (
        """MATCH (source)
WHERE source:A OR source:B
OPTIONAL MATCH (source)-->(target)
WHERE target:A OR target:B
RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target)})"
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_single_multi_graph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A", relationships="REL")

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g",
        data_config={"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"], "relationshipType": "REL"},
    )

    assert (
        runner.last_query()
        == """MATCH (source:A)-[:REL]->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_single_multi_graph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A", relationships="REL", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == dict(
        graph_name="g",
        data_config={"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"], "relationshipType": "REL"},
    )

    assert (
        runner.last_query()
        == """MATCH (source:A)
OPTIONAL MATCH (source)-[:REL]->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_multiple_multi_graph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], relationships=["REL1", "REL2"])

    assert G.name() == "g"
    assert runner.last_params() == dict(graph_name="g")

    assert (
        runner.last_query()
        == """MATCH (source)-[rel:REL1|REL2]->(target)
WHERE (source:A OR source:B) AND (target:A OR target:B)
RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target), "
        "relationshipTypes: type(rel)})"
    )
