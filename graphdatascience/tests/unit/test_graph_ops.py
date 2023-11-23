import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


def test_project_graph_native(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "A", "R", readConcurrency=2)
    assert G.name() == "g"

    assert runner.last_query() == "CALL gds.graph.project($graph_name, $node_spec, $relationship_spec, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "A",
        "relationship_spec": "R",
        "config": {"readConcurrency": 2},
    }


def test_project_graph_native_estimate(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.project.estimate("A", "R", readConcurrency=2)

    assert runner.last_query() == "CALL gds.graph.project.estimate($node_spec, $relationship_spec, $config)"
    assert runner.last_params() == {
        "node_spec": "A",
        "relationship_spec": "R",
        "config": {"readConcurrency": 2},
    }


def test_project_graph_cypher(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project.cypher("g", "RETURN 0 as id", "RETURN 0 as source, 0 as target", readConcurrency=2)
    assert G.name() == "g"

    assert runner.last_query() == "CALL gds.graph.project.cypher($graph_name, $node_spec, $relationship_spec, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
        "config": {"readConcurrency": 2},
    }


def test_project_graph_cypher_estimate(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.project.cypher.estimate("RETURN 0 as id", "RETURN 0 as source, 0 as target", readConcurrency=2)

    assert runner.last_query() == "CALL gds.graph.project.cypher.estimate($node_spec, $relationship_spec, $config)"
    assert runner.last_params() == {
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
        "config": {"readConcurrency": 2},
    }


def test_project_beta_subgraph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project("g", "*", "*")
    gds.beta.graph.project.subgraph("s", from_G, "n.x > 1", "*", concurrency=2)

    assert (
        runner.last_query()
        == "CALL "
        + "gds.beta.graph.project.subgraph($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "node_filter": "n.x > 1",
        "relationship_filter": "*",
        "config": {"concurrency": 2},
    }


def test_project_subgraph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project("g", "*", "*")
    gds.graph.filter("s", from_G, "n.x > 1", "*", concurrency=2)

    assert (
        runner.last_query()
        == "CALL " + "gds.graph.filter($graph_name, $from_graph_name, $node_filter, $relationship_filter, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "node_filter": "n.x > 1",
        "relationship_filter": "*",
        "config": {"concurrency": 2},
    }


def test_project_remote(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.project.remoteDb("g", "RETURN gds.graph.project.remote(0, 1, null)")

    assert (
        runner.last_query()
        == "CALL gds.graph.project.remoteDb($graph_name, $query, $token, $host, $remote_database, $config)"
    )
    # injection of token and host into the params is done by the actual query runner
    assert runner.last_params() == {
        "graph_name": "g",
        "token": "<>",
        "host": "<>",
        "query": "RETURN gds.graph.project.remote(0, 1, null)",
        "remote_database": "dummy",
        "config": {},
    }


def test_graph_list(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.list()

    assert runner.last_query() == "CALL gds.graph.list()"
    assert runner.last_params() == {}

    G, _ = gds.graph.project("g", "A", "R")

    gds.graph.list(G)

    assert runner.last_query() == "CALL gds.graph.list($graph_name)"
    assert runner.last_params() == {"graph_name": G.name()}


def test_graph_exists(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.exists("g")

    assert runner.last_query() == "CALL gds.graph.exists($graph_name)"
    assert runner.last_params() == {"graph_name": "g"}


def test_graph_export(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.export(G, dbName="db", batchSize=10)
    assert runner.last_query() == "CALL gds.graph.export($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "config": {"dbName": "db", "batchSize": 10},
    }


def test_graph_export_csv(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.beta.graph.export.csv(G, exportName="fileName")
    assert runner.last_query() == "CALL gds.beta.graph.export.csv($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "config": {"exportName": "fileName"},
    }

    gds.beta.graph.export.csv.estimate(G, exportName="fileName")
    assert runner.last_query() == "CALL gds.beta.graph.export.csv.estimate($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "config": {"exportName": "fileName"},
    }


def test_graph_streamNodeProperty(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.streamNodeProperty(G, "dummyProp", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.streamNodeProperty($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamNodeProperty(G, "dummyProp", "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.streamNodeProperty($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_nodeProperty_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.nodeProperty.stream(G, "dummyProp", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperty.stream($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.nodeProperty.stream(G, "dummyProp", "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperty.stream($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


def test_graph_streamNodeProperties(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    runner.set__mock_result(DataFrame([{"nodeId": 0, "dummyProp": 2}]))

    gds.graph.streamNodeProperties(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.streamNodeProperties($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamNodeProperties(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.streamNodeProperties($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_nodeProperties_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    runner.set__mock_result(DataFrame([{"nodeId": 0, "dummyProp": 2}]))

    gds.graph.nodeProperties.stream(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperties.stream($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.nodeProperties.stream(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperties.stream($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


def test_graph_streamRelationshipProperty(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.streamRelationshipProperty(G, "dummyProp", concurrency=2)
    assert (
        runner.last_query() == "CALL gds.graph.streamRelationshipProperty($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamRelationshipProperty(G, "dummyProp", "dummyType", concurrency=2)
    assert (
        runner.last_query() == "CALL gds.graph.streamRelationshipProperty($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": "dummyType",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_relationshipProperty_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.relationshipProperty.stream(G, "dummyProp", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationshipProperty.stream($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.relationshipProperty.stream(G, "dummyProp", "dummyType", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationshipProperty.stream($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": "dummyType",
        "config": {"concurrency": 2},
    }


def test_graph_streamRelationshipProperties(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    result_df = DataFrame(
        [
            {
                "sourceNodeId": 0,
                "targetNodeId": 1,
                "relationshipType": "REL",
                "relationshipProperty": "dummyProp",
                "propertyValue": 2,
            }
        ]
    )

    runner.set__mock_result(result_df)

    gds.graph.streamRelationshipProperties(G, ["dummyProp"], concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamRelationshipProperties($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamRelationshipProperties(G, ["dummyProp"], "dummyType", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamRelationshipProperties($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyType",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_relationshipProperties_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    result_df = DataFrame(
        [
            {
                "sourceNodeId": 0,
                "targetNodeId": 1,
                "relationshipType": "REL",
                "relationshipProperty": "dummyProp",
                "propertyValue": 2,
            }
        ]
    )

    runner.set__mock_result(result_df)

    gds.graph.relationshipProperties.stream(G, ["dummyProp"], concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationshipProperties.stream($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.relationshipProperties.stream(G, ["dummyProp"], "dummyType", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationshipProperties.stream($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyType",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_graph_relationshipProperties_write(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.relationshipProperties.write(
        G,
        "dummyType",
        ["dummyProp", "dummyProp2"],
    )
    assert (
        runner.last_query()
        == "CALL gds.graph.relationshipProperties.write($graph_name, $relationship_type, "
        + "$relationship_properties, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "relationship_type": "dummyType",
        "relationship_properties": ["dummyProp", "dummyProp2"],
        "config": {},
    }


def test_graph_writeNodeProperties(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.writeNodeProperties(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.writeNodeProperties($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.writeNodeProperties(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.writeNodeProperties($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_nodeProperties_write(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.nodeProperties.write(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperties.write($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.nodeProperties.write(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperties.write($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


def test_graph_writeRelationship(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.writeRelationship(G, "dummyType", "dummyProp", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.writeRelationship($graph_name, $relationship_type, $relationship_property, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "relationship_type": "dummyType",
        "relationship_property": "dummyProp",
        "config": {"concurrency": 2},
    }

    gds.graph.writeRelationship(G, "dummyType", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.writeRelationship($graph_name, $relationship_type, $relationship_property, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "relationship_type": "dummyType",
        "relationship_property": "",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_relationship_write(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.relationship.write(G, "dummyType", "dummyProp", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationship.write($graph_name, $relationship_type, $relationship_property, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "relationship_type": "dummyType",
        "relationship_property": "dummyProp",
        "config": {"concurrency": 2},
    }

    gds.graph.relationship.write(G, "dummyType", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.relationship.write($graph_name, $relationship_type, $relationship_property, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "relationship_type": "dummyType",
        "relationship_property": "",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 5, 0)])
def test_graph_nodeLabel_write(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.nodeLabel.write(G, "TestLabel", nodeFilter="n.score > 1.0")
    assert runner.last_query() == "CALL gds.graph.nodeLabel.write($graph_name, $node_label, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_label": "TestLabel",
        "config": {"nodeFilter": "n.score > 1.0"},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 5, 0)])
def test_graph_nodeLabel_mutate(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.nodeLabel.mutate(G, "TestLabel", nodeFilter="n.score > 1.0")
    assert runner.last_query() == "CALL gds.graph.nodeLabel.mutate($graph_name, $node_label, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_label": "TestLabel",
        "config": {"nodeFilter": "n.score > 1.0"},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 0, 0)])
def test_graph_removeNodeProperties_20(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.removeNodeProperties(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert runner.last_query() == "CALL gds.graph.removeNodeProperties($graph_name, $properties, $entities, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 1, 0)])
def test_graph_removeNodeProperties_21(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.removeNodeProperties(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.removeNodeProperties($graph_name, $properties, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "config": {"concurrency": 2},
    }


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_nodeProperties_remove_drop(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.nodeProperties.drop(G, ["dummyProp"], concurrency=2)
    assert runner.last_query() == "CALL gds.graph.nodeProperties.drop($graph_name, $properties, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "config": {"concurrency": 2},
    }


def test_graph_deleteRelationships(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.deleteRelationships(G, "REL_A")
    assert runner.last_query() == "CALL gds.graph.deleteRelationships($graph_name, $relationship_type)"
    assert runner.last_params() == {"graph_name": "g", "relationship_type": "REL_A"}


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_relationships_drop(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.graph.relationships.drop(G, "REL_A")
    assert runner.last_query() == "CALL gds.graph.relationships.drop($graph_name, $relationship_type)"
    assert runner.last_params() == {"graph_name": "g", "relationship_type": "REL_A"}


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_property_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.alpha.graph.graphProperty.stream(G, "prop")
    assert runner.last_query() == "CALL gds.alpha.graph.graphProperty.stream($graph_name, $graph_property, $config)"
    assert runner.last_params() == {"graph_name": "g", "graph_property": "prop", "config": {}}


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_property_drop(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.alpha.graph.graphProperty.drop(G, "prop")
    assert runner.last_query() == "CALL gds.alpha.graph.graphProperty.drop($graph_name, $graph_property, $config)"
    assert runner.last_params() == {"graph_name": "g", "graph_property": "prop", "config": {}}


@pytest.mark.parametrize("server_version", [ServerVersion(2, 2, 0)])
def test_graph_relationships_stream(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project("g", "*", "*")

    gds.beta.graph.relationships.stream(G, ["REL_A"])
    assert runner.last_query() == "CALL gds.beta.graph.relationships.stream($graph_name, $relationship_types, $config)"
    assert runner.last_params() == {"graph_name": "g", "relationship_types": ["REL_A"], "config": {}}


def test_beta_graph_generate(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.beta.graph.generate("g", 1337, 42, orientation="NATURAL")

    assert runner.last_query() == "CALL gds.beta.graph.generate($graph_name, $node_count, $average_degree, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_count": 1337,
        "average_degree": 42,
        "config": {"orientation": "NATURAL"},
    }


def test_graph_generate(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.generate("g", 1337, 42, orientation="NATURAL")

    assert runner.last_query() == "CALL gds.graph.generate($graph_name, $node_count, $average_degree, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "node_count": 1337,
        "average_degree": 42,
        "config": {"orientation": "NATURAL"},
    }


def test_alpha_graph_sample_rwr(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project("g", "*", "*")

    gds.alpha.graph.sample.rwr("s", from_G, samplingRatio=0.9, concurrency=7)

    assert runner.last_query() == "CALL gds.alpha.graph.sample.rwr($graph_name, $from_graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "config": {"samplingRatio": 0.9, "concurrency": 7},
    }


def test_graph_sample_rwr(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project("g", "*", "*")
    gds.graph.sample.rwr("s", from_G, samplingRatio=0.9, concurrency=7)

    assert runner.last_query() == "CALL gds.graph.sample.rwr($graph_name, $from_graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "config": {"samplingRatio": 0.9, "concurrency": 7},
    }


def test_graph_sample_cnarw(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project("g", "*", "*")
    gds.graph.sample.cnarw("s", from_G, samplingRatio=0.9, concurrency=7)

    assert runner.last_query() == "CALL gds.graph.sample.cnarw($graph_name, $from_graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "s",
        "from_graph_name": "g",
        "config": {"samplingRatio": 0.9, "concurrency": 7},
    }


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 6, 0))
def test_remote_projection_on_specific_database(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.set_database("bar")
    G, _ = gds.graph.project.remoteDb("g", "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)")

    assert (
        runner.last_query()
        == "CALL gds.graph.project.remoteDb($graph_name, $query, $token, $host, $remote_database, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "token": "<>",
        "host": "<>",
        "query": "MATCH (n)-->(m) RETURN gds.graph.project.remote(n, m)",
        "remote_database": "bar",
        "config": {},
    }
