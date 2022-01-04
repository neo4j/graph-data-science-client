from gdsclient.graph_data_science import GraphDataScience

from .conftest import CollectingQueryRunner


def test_project_graph_native(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    G = gds.graph.project("g", "A", "R")
    assert G.name() == "g"

    assert (
        runner.last_query()
        == "CALL gds.graph.project($graph_name, $node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_project_graph_native_estimate(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    gds.graph.project.estimate("A", "R")

    assert (
        runner.last_query()
        == "CALL gds.graph.project.estimate($node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "node_spec": "A",
        "relationship_spec": "R",
    }


def test_project_graph_cypher(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    G = gds.graph.project.cypher(
        "g", "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )
    assert G.name() == "g"

    assert (
        runner.last_query()
        == "CALL gds.graph.project.cypher($graph_name, $node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }


def test_project_graph_cypher_estimate(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    gds.graph.project.cypher.estimate(
        "RETURN 0 as id", "RETURN 0 as source, 0 as target"
    )

    assert (
        runner.last_query()
        == "CALL gds.graph.project.cypher.estimate($node_spec, $relationship_spec)"
    )
    assert runner.last_params() == {
        "node_spec": "RETURN 0 as id",
        "relationship_spec": "RETURN 0 as source, 0 as target",
    }


def test_project_subgraph(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    from_G = gds.graph.project("g", "*", "*")
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


def test_graph_list(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.list()

    assert runner.last_query() == "CALL gds.graph.list()"
    assert runner.last_params() == {}

    G = gds.graph.project("g", "A", "R")

    gds.graph.list(G)

    assert runner.last_query() == "CALL gds.graph.list($graph_name)"
    assert runner.last_params() == {"graph_name": G.name()}


def test_graph_exists(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    gds.graph.exists("g")

    assert runner.last_query() == "CALL gds.graph.exists($graph_name)"
    assert runner.last_params() == {"graph_name": "g"}


def test_graph_export(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G = gds.graph.project("g", "*", "*")
    gds.graph.export(G, dbName="db", batchSize=10)

    assert runner.last_query() == "CALL gds.graph.export($graph_name, $config)"
    assert runner.last_params() == {
        "graph_name": "g",
        "config": {"dbName": "db", "batchSize": 10},
    }


def test_graph_streamNodeProperty(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    G = gds.graph.project("g", "*", "*")

    gds.graph.streamNodeProperty(G, "dummyProp", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamNodeProperty($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamNodeProperty(G, "dummyProp", "dummyLabel", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamNodeProperty($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": "dummyProp",
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }


def test_graph_streamNodeProperties(
    runner: CollectingQueryRunner, gds: GraphDataScience
) -> None:
    G = gds.graph.project("g", "*", "*")

    gds.graph.streamNodeProperties(G, ["dummyProp"], concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamNodeProperties($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": ["*"],
        "config": {"concurrency": 2},
    }

    gds.graph.streamNodeProperties(G, ["dummyProp"], "dummyLabel", concurrency=2)
    assert (
        runner.last_query()
        == "CALL gds.graph.streamNodeProperties($graph_name, $properties, $entities, $config)"
    )
    assert runner.last_params() == {
        "graph_name": "g",
        "properties": ["dummyProp"],
        "entities": "dummyLabel",
        "config": {"concurrency": 2},
    }
