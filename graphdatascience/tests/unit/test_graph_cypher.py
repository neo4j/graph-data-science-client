import pytest

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_run_project(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.run_project("MATCH (s)-->(t) RETURN gds.graph.project('gg', s, t)")

    assert G.name() == "gg"
    assert runner.last_params() == {}

    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project('gg', s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_run_project_with_return_as(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.run_project("MATCH (s)-->(t) RETURN gds.graph.project('gg', s, t) AS graph")

    assert G.name() == "gg"
    assert runner.last_params() == {}

    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project('gg', s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_run_project_with_graph_name_parameter(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.run_project(
        "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)", params={"graph_name": "gg"}
    )

    assert G.name() == "gg"
    assert runner.last_params() == {"graph_name": "gg"}

    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_all(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g")

    assert G.name() == "g"
    assert runner.last_params() == {"graph_name": "g"}

    assert (
        runner.last_query()
        == """MATCH (source)-->(target)
RETURN gds.graph.project($graph_name, source, target)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == {"graph_name": "g"}

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
    assert runner.last_params() == {"graph_name": "g"}

    assert (
        runner.last_query()
        == """MATCH (source)<--(target)
RETURN gds.graph.project($graph_name, source, target)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_single_node_label(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A")

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"]},
    }

    assert (
        runner.last_query()
        == """MATCH (source:A)-->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_single_node_label(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"]},
    }

    assert (
        runner.last_query()
        == """MATCH (source:A)
OPTIONAL MATCH (source)-->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_single_node_label_alias(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes={"Target": "Label"})

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["Target"], "targetNodeLabels": ["Target"]},
    }

    assert (
        runner.last_query()
        == """MATCH (source:Label)-->(target:Label)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_multiple_node_labels_and(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="AND")

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A", "B"], "targetNodeLabels": ["A", "B"]},
    }

    assert (
        runner.last_query()
        == """MATCH (source:A:B)-->(target:A:B)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_multiple_node_labels_and(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes=["A", "B"], combine_labels_with="AND", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A", "B"], "targetNodeLabels": ["A", "B"]},
    }

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
    assert runner.last_params() == {"graph_name": "g"}

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
    assert runner.last_params() == {"graph_name": "g"}

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
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"], "relationshipType": "REL"},
    }

    assert (
        runner.last_query()
        == """MATCH (source:A)-[:REL]->(target:A)
RETURN gds.graph.project($graph_name, source, target, $data_config)"""
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_disconnected_nodes_single_multi_graph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", nodes="A", relationships="REL", allow_disconnected_nodes=True)

    assert G.name() == "g"
    assert runner.last_params() == {
        "graph_name": "g",
        "data_config": {"sourceNodeLabels": ["A"], "targetNodeLabels": ["A"], "relationshipType": "REL"},
    }

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
    assert runner.last_params() == {"graph_name": "g"}

    assert (
        runner.last_query()
        == """MATCH (source)-[rel:REL1|REL2]->(target)
WHERE (source:A OR source:B) AND (target:A OR target:B)
RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target), "
        "relationshipTypes: type(rel)})"
    )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_node_properties(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project(
        "g", nodes={"L1": ["prop1"], "L2": ["prop2", "prop3"], "L3": {"prop4": True, "prop5": {}}}
    )

    assert G.name() == "g"
    assert runner.last_params() == {"graph_name": "g"}

    assert runner.last_query() == (
        """MATCH (source)-->(target)
WHERE (source:L1 OR source:L2 OR source:L3) AND (target:L1 OR target:L2 OR target:L3)
WITH source, target
CASE
WHEN 'L1' in labels(source) THEN [source {.prop1}]
WHEN 'L2' in labels(source) THEN [source {.prop2, .prop3}]
WHEN 'L3' in labels(source) THEN [source {.prop4, .prop5}]
END AS sourceNodeProperties
CASE
WHEN 'L1' in labels(target) THEN [target {.prop1}]
WHEN 'L2' in labels(target) THEN [target {.prop2, .prop3}]
WHEN 'L3' in labels(target) THEN [target {.prop4, .prop5}]
END AS targetNodeProperties
RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target), "
        "sourceNodeProperties: sourceNodeProperties, "
        "targetNodeProperties: targetNodeProperties})"
    )


@pytest.mark.skip(reason="Not implemented yet")
@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_node_properties_alias(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project(
        "g", nodes={"A": {"target_prop1": "source_prop1", "target_prop2": {"property_key": "source_prop2"}}}
    )

    assert G.name() == "g"
    assert runner.last_params() == {"graph_name": "g"}

    assert runner.last_query() == (
        """MATCH (source:A)-->(target:A)
WITH source, target, """
        "[{target_prop1: source.source_prop1, target_prop1: source.source_prop2}] AS sourceNodeProperties"
        """[{target_prop1: target.source_prop1, target_prop1: target.source_prop2}] AS targetNodeProperties
 RETURN gds.graph.project($graph_name, source, target, {"""
        "sourceNodeLabels: labels(source), "
        "targetNodeLabels: labels(target), "
        "sourceNodeProperties: sourceNodeProperties, "
        "targetNodeProperties: targetNodeProperties})"
    )
