import warnings
from typing import Generator

import numpy as np
import pytest
from pandas import DataFrame, Series

from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.arrow_query_runner import ArrowQueryRunner
from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.integration.conftest import AUTH, DB, URI

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
def run_around_tests(gds: GraphDataScience) -> Generator[None, None, None]:
    # Runs before each test
    gds.run_cypher(
        """
        CREATE
        (a: Node {x: 1, y: 2, z: [42], name: "nodeA"}),
        (b: Node {x: 2, y: 3, z: [1337], name: "nodeB"}),
        (c: Node {x: 3, y: 4, z: [9], name: "nodeC"}),
        (a)-[:REL {relX: 4, relY: 5}]->(b),
        (a)-[:REL {relX: 5, relY: 6}]->(c),
        (b)-[:REL {relX: 6, relY: 7}]->(c),
        (b)-[:REL2]->(c)
        """
    )

    yield  # Test runs here

    # Runs after each test
    gds.run_cypher("MATCH (n) DETACH DELETE n")
    gds.graph.drop(GRAPH_NAME, failIfMissing=False)


def test_project_graph_native(gds: GraphDataScience) -> None:
    G, result = gds.graph.project(GRAPH_NAME, "*", "*")
    assert G.name() == GRAPH_NAME
    assert result["graphName"] == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result["exists"]


def test_project_graph_native_estimate(gds: GraphDataScience) -> None:
    result = gds.graph.project.estimate("*", "*")

    assert result["requiredMemory"]


def test_project_graph_cypher(gds: GraphDataScience) -> None:
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"

    if gds.server_version() >= ServerVersion(2, 4, 0):
        with pytest.warns(DeprecationWarning):
            G, result = gds.graph.project.cypher(GRAPH_NAME, node_query, relationship_query)
    else:
        G, result = gds.graph.project.cypher(GRAPH_NAME, node_query, relationship_query)

    assert G.name() == GRAPH_NAME
    assert result["graphName"] == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result["exists"]


def test_project_graph_cypher_estimate(gds: GraphDataScience) -> None:
    node_query = "MATCH (n:Node) RETURN id(n) as id"
    relationship_query = "MATCH (n:Node)-->(m:Node) RETURN id(n) as source, id(m) as target, 'T' as type"

    if gds.server_version() >= ServerVersion(2, 5, 0):
        with pytest.warns(DeprecationWarning):
            result = gds.graph.project.cypher.estimate(node_query, relationship_query)
    else:
        result = gds.graph.project.cypher.estimate(node_query, relationship_query)

    assert result["requiredMemory"]


def test_cypher_projection(gds: GraphDataScience) -> None:
    G, result = gds.graph.cypher.project(
        f"MATCH (n:Node) OPTIONAL MATCH (n)-->(m:Node) RETURN gds.graph.project('{GRAPH_NAME}', n, m)"
    )

    assert G.name() == GRAPH_NAME
    assert result["graphName"] == GRAPH_NAME

    result = gds.graph.exists(G.name())
    assert result["exists"]


@pytest.mark.filterwarnings("ignore: One of the labels in your query is not available in the database")
def test_cypher_projection_empty_graph(gds: GraphDataScience) -> None:
    with pytest.raises(ValueError, match="Projected graph cannot be empty"):
        gds.graph.cypher.project("MATCH (n:MISSING_LABEL) RETURN gds.graph.project('some-graph', n, null)")


def test_beta_project_subgraph(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    if gds.server_version() >= ServerVersion(2, 5, 0):
        with pytest.warns(DeprecationWarning):
            sub_G, result = gds.beta.graph.project.subgraph("s", from_G, "n.x > 1", "*", concurrency=2)
    else:
        sub_G, result = gds.beta.graph.project.subgraph("s", from_G, "n.x > 1", "*", concurrency=2)

    assert sub_G.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(sub_G)
    assert result2["nodeCount"][0] == 2

    gds.graph.drop(sub_G)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_project_subgraph(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    sub_G, result = gds.graph.filter("s", from_G, "n.x > 1", "*", concurrency=2)

    assert sub_G.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(sub_G)
    assert result2["nodeCount"][0] == 2

    gds.graph.drop(sub_G)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 4, 0))
def test_sample_rwr(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    rwr_G, result = gds.graph.sample.rwr("s", from_G, samplingRatio=0.6, concurrency=1, randomSeed=42)

    assert rwr_G.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(rwr_G)
    assert result2["nodeCount"][0] == 2

    gds.graph.drop(rwr_G)


@pytest.mark.skip_on_aura  # The alpha procedure is not part of the allowlist
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_sample_rwr_alpha(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    if gds.server_version() >= ServerVersion(2, 4, 0):
        with pytest.warns(DeprecationWarning):
            rwr_G, result = gds.alpha.graph.sample.rwr("s", from_G, samplingRatio=0.6, concurrency=1, randomSeed=42)
    else:
        rwr_G, result = gds.alpha.graph.sample.rwr("s", from_G, samplingRatio=0.6, concurrency=1, randomSeed=42)

    assert rwr_G.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(rwr_G)
    assert result2["nodeCount"][0] == 2

    gds.graph.drop(rwr_G)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 4, 0))
def test_sample_cnarw(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    cnarw_G, result = gds.graph.sample.cnarw("s", from_G, samplingRatio=0.6, concurrency=1, randomSeed=42)

    assert cnarw_G.name() == "s"
    assert result["graphName"] == "s"

    result2 = gds.graph.list(cnarw_G)
    assert result2["nodeCount"][0] == 2

    gds.graph.drop(cnarw_G)


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 4, 0))
def test_sample_cnarw_estimate(runner: QueryRunner, gds: GraphDataScience) -> None:
    from_G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.sample.cnarw.estimate(from_G, samplingRatio=0.6, concurrency=1, randomSeed=42)

    assert result["requiredMemory"]


def test_graph_list(gds: GraphDataScience) -> None:
    result = gds.graph.list()
    assert len(result) == 0

    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    result = gds.graph.list()
    assert len(result) == 1

    result = gds.graph.list(G)
    assert result["graphName"][0] == GRAPH_NAME


def test_graph_exists(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.exists(G.name())
    assert result["exists"]

    result = gds.graph.exists("bogusName")
    assert not result["exists"]


def test_graph_drop(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    result = gds.graph.drop(G, True)
    assert result is not None
    assert result["graphName"] == GRAPH_NAME

    with pytest.raises(Exception):
        gds.graph.drop(G, True)


def test_graph_type_check(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    # Call without providing optional `Graph` without raising
    gds.graph.list()

    # Raise when optional `Graph` is string
    with pytest.raises(
        TypeError,
        match=(
            f"The parameter 'G' takes a `Graph` object, but received string '{G.name()}'. "
            "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
        ),
    ):
        gds.graph.list(G.name())  # type: ignore

    # Raise when second positional from_G `Graph` is string
    with pytest.raises(
        TypeError,
        match=(
            f"The parameter 'from_G' takes a `Graph` object, but received string '{G.name()}'. "
            "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
        ),
    ):
        gds.beta.graph.project.subgraph("s", G.name(), "n.x > 1", "*", concurrency=2)  # type: ignore

    # Raise when first positional G `Graph` is string
    with pytest.raises(
        TypeError,
        match=(
            f"The parameter 'G' takes a `Graph` object, but received string '{G.name()}'. "
            "To resolve a graph name string into a `Graph` object, please use `gds.graph.get`"
        ),
    ):
        gds.graph.list(G.name())  # type: ignore

    result = gds.graph.drop(G, True)
    assert result is not None
    assert result["graphName"] == GRAPH_NAME


@pytest.mark.skip_on_aura
def test_graph_export(runner: QueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    MY_DB_NAME = "testdatabase"
    result = gds.graph.export(G, dbName=MY_DB_NAME, batchSize=10000)

    assert result["graphName"] == GRAPH_NAME
    assert result["dbName"] == MY_DB_NAME

    runner.run_cypher("CREATE DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})
    runner.set_database(MY_DB_NAME)
    node_count = runner.run_cypher("MATCH (n) RETURN COUNT(n) AS c").squeeze()

    assert node_count == 3
    runner.set_database(DB)
    runner.run_cypher("DROP DATABASE $dbName WAIT", {"dbName": MY_DB_NAME})


@pytest.mark.filterwarnings("ignore: The query used a deprecated procedure.")
@pytest.mark.skip_on_aura
def test_beta_graph_export_csv_estimate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    if gds.server_version() >= ServerVersion(2, 5, 0):
        with pytest.warns(DeprecationWarning):
            result = gds.beta.graph.export.csv.estimate(G, exportName="dummy")
    else:
        result = gds.beta.graph.export.csv.estimate(G, exportName="dummy")

    assert result["nodeCount"] == 3


def test_graph_get(gds: GraphDataScience) -> None:
    gds.graph.project(GRAPH_NAME, "*", "*")

    G = gds.graph.get(GRAPH_NAME)
    assert G.name() == GRAPH_NAME

    with pytest.raises(
        ValueError,
        match=f"No projected graph named 'bogusName' exists in current database '{gds.database()}'",
    ):
        gds.graph.get("bogusName")


def test_graph_streamNodeProperty_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds.graph.streamNodeProperty(G, "x", concurrency=2)
    assert {e for e in result["propertyValue"]} == {1, 2, 3}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.nodeProperty.stream(G, "x", concurrency=2)
    assert {e for e in result["propertyValue"]} == {1, 2, 3}


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 2, 0))
def test_graph_streamNodeProperty_with_arrow_no_db() -> None:
    gds = GraphDataScience(URI, auth=AUTH)
    if not isinstance(gds._query_runner, ArrowQueryRunner):
        gds.close()
        pytest.skip("Arrow server not enabled")

    assert not gds.database()
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.raises(ValueError):
        gds.graph.streamNodeProperty(G, "x", concurrency=2)
    gds.close()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_with_arrow_no_db() -> None:
    gds = GraphDataScience(URI, auth=AUTH)
    if not isinstance(gds._query_runner, ArrowQueryRunner):
        gds.close()
        pytest.skip("Arrow server not enabled")

    assert not gds.database()
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.raises(ValueError):
        gds.graph.nodeProperty.stream(G, "x", concurrency=2)
    gds.close()


def test_graph_streamNodeProperty_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamNodeProperty(G, "x", concurrency=2)

    assert {e for e in result["propertyValue"]} == {1, 2, 3}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds_without_arrow.graph.nodeProperty.stream(G, "x", concurrency=2)

    assert {e for e in result["propertyValue"]} == {1, 2, 3}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_db_properties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.nodeProperty.stream(G, "x", db_node_properties=["z", "name"], concurrency=2)

    assert {"nodeId", "nodeProperty", "propertyValue"}.issubset(set(result.keys()))

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    z_values = result[result.nodeProperty == "z"]
    assert len(z_values) == 3
    for e in z_values["propertyValue"]:
        assert e in [[9], [42], [1337]]

    name_values = result[result.nodeProperty == "name"]
    assert {e for e in name_values["propertyValue"]} == {"nodeA", "nodeB", "nodeC"}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_db_properties_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds_without_arrow.graph.nodeProperty.stream(G, "x", db_node_properties=["z", "name"], concurrency=2)

    assert {"nodeId", "nodeProperty", "propertyValue"}.issubset(set(result.keys()))

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    z_values = result[result.nodeProperty == "z"]
    assert len(z_values) == 3
    for e in z_values["propertyValue"]:
        assert e in [[9], [42], [1337]]

    name_values = result[result.nodeProperty == "name"]
    assert {e for e in name_values["propertyValue"]} == {"nodeA", "nodeB", "nodeC"}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_raise_error_with_duplicate_keys(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.raises(ValueError, match="Duplicate property keys '{'x'}' in db_node_properties and node_properties."):
        gds.graph.nodeProperty.stream(G, "x", db_node_properties=["x", "z", "name"], concurrency=2)


def test_graph_nodeProperty_fail_on_duplicate_node_properties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    with pytest.raises(ValueError, match="The provided db_node_properties contain duplicate property names"):
        gds.graph.nodeProperty.stream(G, "x", db_node_properties=["z", "z", "name"], concurrency=2)


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperties_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    # also check duplicates will be filtered out
    result = gds.graph.nodeProperties.stream(G, ["x", "y"], db_node_properties=["z", "name"], concurrency=2)

    assert list(result.keys()) == ["nodeId", "nodeProperty", "propertyValue"]
    assert result.shape == (G.node_count() * 4, 3)  # 4 properties

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    y_values = result[result.nodeProperty == "y"]
    assert {e for e in y_values["propertyValue"]} == {2, 3, 4}

    z_values = result[result.nodeProperty == "z"]
    assert len(z_values) == 3
    for e in z_values["propertyValue"]:
        assert e in [[9], [42], [1337]]

    name_values = result[result.nodeProperty == "name"]
    assert {e for e in name_values["propertyValue"]} == {"nodeA", "nodeB", "nodeC"}


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_graph_nodeProperties_stream_listNodeLabels_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x"]}}, "*")

    result = gds.graph.nodeProperties.stream(G, ["x"], concurrency=2, listNodeLabels=True)

    assert list(result.keys()) == ["nodeId", "nodeLabels", "nodeProperty", "propertyValue"]

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    assert [e for e in result["nodeLabels"]] == [["Node"], ["Node"], ["Node"]]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_graph_nodeProperties_stream_listNodeLabels(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x"]}}, "*")

    result = gds_without_arrow.graph.nodeProperties.stream(G, ["x"], concurrency=2, listNodeLabels=True)

    assert list(result.keys()) == ["nodeId", "nodeProperty", "propertyValue", "nodeLabels"]

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    assert [e for e in result["nodeLabels"]] == [["Node"], ["Node"], ["Node"]]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_graph_nodeProperties_stream_listNodeLabels_with_seperate_cols(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    result = gds_without_arrow.graph.nodeProperties.stream(
        G, ["x", "y"], concurrency=2, listNodeLabels=True, separate_property_columns=True
    )

    expected_columns = ["nodeId", "x", "y", "nodeLabels"]
    assert list(result.keys()) == expected_columns

    assert result.shape == (G.node_count(), len(expected_columns))

    assert [e for e in result["nodeLabels"]] == [["Node"], ["Node"], ["Node"]]


def test_graph_streamNodeProperties_with_arrow_separate_property_columns(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds.graph.streamNodeProperties(G, ["x", "y"], separate_property_columns=True, concurrency=2)
    assert list(result.keys()) == ["nodeId", "x", "y"]
    assert {e for e in result["x"]} == {1, 2, 3}
    assert {e for e in result["y"]} == {2, 3, 4}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperties_stream_with_arrow_separate_property_columns(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    result = gds.graph.nodeProperties.stream(
        G, ["x", "y"], db_node_properties=["z", "name"], separate_property_columns=True, concurrency=2
    )
    assert set(result.keys()) == {"nodeId", "x", "y", "z", "name"}
    assert {e for e in result["x"]} == {1, 2, 3}
    assert {e for e in result["y"]} == {2, 3, 4}
    assert len(result["z"]) == 3
    for e in result["z"]:
        assert e in [[9], [42], [1337]]
    assert {e for e in result["name"]} == {"nodeA", "nodeB", "nodeC"}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperties_stream_raise_error_with_duplicate_keys(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    with pytest.raises(ValueError, match="Duplicate property keys '{'y'}' in db_node_properties and node_properties."):
        gds.graph.nodeProperties.stream(
            G, ["x", "y"], db_node_properties=["y", "z", "name"], separate_property_columns=True, concurrency=2
        )


def test_graph_streamNodeProperties_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamNodeProperties(G, ["x", "y"], concurrency=2)

    assert {"nodeId", "nodeProperty", "propertyValue"}.issubset(set(result.keys()))

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    y_values = result[result.nodeProperty == "y"]
    assert {e for e in y_values["propertyValue"]} == {2, 3, 4}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperties_stream_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    result = gds_without_arrow.graph.nodeProperties.stream(
        G, ["x", "y"], db_node_properties=["z", "name"], concurrency=2
    )

    assert {"nodeId", "nodeProperty", "propertyValue", "nodeLabels"} == set(
        result.keys()
    )  # empty nodeLabels column even if listNodeLabels = False
    assert result.shape == (G.node_count() * 4, 3 + 1)  # 4 properties

    x_values = result[result.nodeProperty == "x"]
    assert {e for e in x_values["propertyValue"]} == {1, 2, 3}

    y_values = result[result.nodeProperty == "y"]
    assert {e for e in y_values["propertyValue"]} == {2, 3, 4}

    z_values = result[result.nodeProperty == "z"]
    assert len(z_values) == 3
    for e in z_values["propertyValue"]:
        assert e in [[9], [42], [1337]]

    name_values = result[result.nodeProperty == "name"]
    assert {e for e in name_values["propertyValue"]} == {"nodeA", "nodeB", "nodeC"}


def test_graph_nodeProperties_fail_on_duplicate_node_properties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    with pytest.raises(ValueError, match="The provided node_properties contain duplicate property names"):
        gds.graph.nodeProperties.stream(G, ["x", "x", "y"], db_node_properties=["z", "name"], concurrency=2)

    with pytest.raises(ValueError, match="The provided db_node_properties contain duplicate property names"):
        gds.graph.nodeProperties.stream(G, ["x", "y"], db_node_properties=["z", "z", "name"], concurrency=2)


def test_graph_streamNodeProperties_without_arrow_separate_property_columns(
    gds_without_arrow: GraphDataScience,
) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "z"]}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamNodeProperties(
            G, ["x", "z"], separate_property_columns=True, concurrency=2
        )

    assert list(result.keys()) == ["nodeId", "x", "z"]

    assert {e for e in result["x"]} == {1, 2, 3}

    assert len(result["z"]) == 3
    for e in result["z"]:
        assert e in [[9], [42], [1337]]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperties_stream_without_arrow_separate_property_columns(
    gds_without_arrow: GraphDataScience,
) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, {"Node": {"properties": ["x", "y"]}}, "*")

    result = gds_without_arrow.graph.nodeProperties.stream(
        G, ["x", "y"], db_node_properties=["z", "name"], separate_property_columns=True, concurrency=2
    )

    assert set(result.keys()) == {"nodeId", "x", "y", "z", "name"}

    assert {e for e in result["x"]} == {1, 2, 3}
    assert {e for e in result["y"]} == {2, 3, 4}
    assert len(result["z"]) == 3
    for e in result["z"]:
        assert e in [[42], [1337], [9]]
    assert {e for e in result["name"]} == {"nodeA", "nodeB", "nodeC"}


def test_graph_streamRelationshipProperty_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    with pytest.warns(DeprecationWarning):
        result = gds.graph.streamRelationshipProperty(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperty_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    result = gds.graph.relationshipProperty.stream(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


def test_graph_streamRelationshipProperty_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamRelationshipProperty(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperty_stream_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": "relX"}})

    result = gds_without_arrow.graph.relationshipProperty.stream(G, "relX", concurrency=2)
    assert {e for e in result["propertyValue"]} == {4, 5, 6}


def test_graph_streamRelationshipProperties_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    with pytest.warns(DeprecationWarning):
        result = gds.graph.streamRelationshipProperties(G, ["relX", "relY"], concurrency=2)

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relationshipProperty",
        "propertyValue",
    ]

    x_values = result[result.relationshipProperty == "relX"]
    assert {e for e in x_values["propertyValue"]} == {4, 5, 6}
    y_values = result[result.relationshipProperty == "relY"]
    assert {e for e in y_values["propertyValue"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds.graph.relationshipProperties.stream(G, ["relX", "relY"], concurrency=2)

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relationshipProperty",
        "propertyValue",
    ]

    x_values = result[result.relationshipProperty == "relX"]
    assert {e for e in x_values["propertyValue"]} == {4, 5, 6}
    y_values = result[result.relationshipProperty == "relY"]
    assert {e for e in y_values["propertyValue"]} == {5, 6, 7}


def test_graph_streamRelationshipProperties_with_arrow_separate_property_columns(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    with pytest.warns(DeprecationWarning):
        result = gds.graph.streamRelationshipProperties(
            G, ["relX", "relY"], separate_property_columns=True, concurrency=2
        )

    assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType", "relX", "relY"]
    assert {e for e in result["relX"]} == {4, 5, 6}
    assert {e for e in result["relY"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_with_arrow_separate_property_columns(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds.graph.relationshipProperties.stream(G, ["relX", "relY"], separate_property_columns=True, concurrency=2)

    assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType", "relX", "relY"]
    assert {e for e in result["relX"]} == {4, 5, 6}
    assert {e for e in result["relY"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_with_arrow_rel_as_str(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds.graph.relationshipProperties.stream(G, ["relX", "relY"], "REL", concurrency=2)

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relationshipProperty",
        "propertyValue",
    ]

    x_values = result[result.relationshipProperty == "relX"]
    assert {e for e in x_values["propertyValue"]} == {4, 5, 6}
    y_values = result[result.relationshipProperty == "relY"]
    assert {e for e in y_values["propertyValue"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_with_arrow_rel_as_str_sep(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds.graph.relationshipProperties.stream(
        G, ["relX", "relY"], "REL", separate_property_columns=True, concurrency=2
    )

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relX",
        "relY",
    ]

    assert {e for e in result["relX"]} == {4, 5, 6}
    assert {e for e in result["relY"]} == {5, 6, 7}


def test_graph_streamRelationshipProperties_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamRelationshipProperties(G, ["relX", "relY"], concurrency=2)

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relationshipProperty",
        "propertyValue",
    ]

    x_values = result[result.relationshipProperty == "relX"]
    assert {e for e in x_values["propertyValue"]} == {4, 5, 6}
    y_values = result[result.relationshipProperty == "relY"]
    assert {e for e in y_values["propertyValue"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds_without_arrow.graph.relationshipProperties.stream(G, ["relX", "relY"], concurrency=2)

    assert list(result.keys()) == [
        "sourceNodeId",
        "targetNodeId",
        "relationshipType",
        "relationshipProperty",
        "propertyValue",
    ]

    x_values = result[result.relationshipProperty == "relX"]
    assert {e for e in x_values["propertyValue"]} == {4, 5, 6}
    y_values = result[result.relationshipProperty == "relY"]
    assert {e for e in y_values["propertyValue"]} == {5, 6, 7}


def test_graph_streamRelationshipProperties_without_arrow_separate_property_columns(
    gds_without_arrow: GraphDataScience,
) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    with pytest.warns(DeprecationWarning):
        result = gds_without_arrow.graph.streamRelationshipProperties(
            G, ["relX", "relY"], separate_property_columns=True, concurrency=2
        )

    assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType", "relX", "relY"]
    assert {e for e in result["relX"]} == {4, 5, 6}
    assert {e for e in result["relY"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationshipProperties_stream_without_arrow_separate_property_columns(
    gds_without_arrow: GraphDataScience,
) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", {"REL": {"properties": ["relX", "relY"]}})

    result = gds_without_arrow.graph.relationshipProperties.stream(
        G, ["relX", "relY"], separate_property_columns=True, concurrency=2
    )

    assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType", "relX", "relY"]
    assert {e for e in result["relX"]} == {4, 5, 6}
    assert {e for e in result["relY"]} == {5, 6, 7}


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationships_stream_without_arrow(gds_without_arrow: GraphDataScience) -> None:
    G, _ = gds_without_arrow.graph.project(GRAPH_NAME, "*", ["REL", "REL2"])

    if gds_without_arrow.server_version() >= ServerVersion(2, 5, 0):
        result = gds_without_arrow.graph.relationships.stream(G, ["REL", "REL2"])
    else:
        result = gds_without_arrow.beta.graph.relationships.stream(G, ["REL", "REL2"])

    warnings.filterwarnings(
        "ignore", category=DeprecationWarning, message="The query used a deprecated function: `id`."
    )
    expected = gds_without_arrow.run_cypher(
        "MATCH (n)-[r]->(m) RETURN id(n) AS src_id, id(m) AS trg_id, type(r) AS rel_type"
    )

    # Pandas 2.2.0 deprecated an API used by the following assertion (in the underlying impl of pandas)
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message=r"Passing a BlockManager to TopologyDataFrame is deprecated",
    )

    assert result.shape[0] == expected.shape[0]
    for _, row in expected.iterrows():
        assert (result == np.array(row)).all(1).any()

    assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType"]

    by_rel_type = result.by_rel_type()

    num_rels = 0
    for rel_type, matrix in by_rel_type.items():
        num_rels += len(matrix[0])
        for i in range(len(matrix[0])):
            row = Series([by_rel_type[rel_type][0][i], by_rel_type[rel_type][1][i], rel_type])
            assert (result == np.array(row)).all(1).any()

    assert num_rels == result.shape[0]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationships_stream_with_arrow(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", ["REL", "REL2"])

    if gds.server_version() >= ServerVersion(2, 5, 0):
        result = gds.graph.relationships.stream(G, ["REL", "REL2"])
    else:
        result = gds.beta.graph.relationships.stream(G, ["REL", "REL2"])

    with pytest.warns(DeprecationWarning):
        expected = gds.run_cypher("MATCH (n)-[r]->(m) RETURN id(n) AS src_id, id(m) AS trg_id, type(r) AS rel_type")

        assert list(result.keys()) == ["sourceNodeId", "targetNodeId", "relationshipType"]

        assert result.shape[0] == expected.shape[0]
        for _, row in expected.iterrows():
            assert (result == np.array(row)).all(1).any()

    by_rel_type = result.by_rel_type()

    num_rels = 0
    for rel_type, matrix in by_rel_type.items():
        num_rels += len(matrix[0])
        for i in range(len(matrix[0])):
            row = Series([by_rel_type[rel_type][0][i], by_rel_type[rel_type][1][i], rel_type])
            assert (result == np.array(row)).all(1).any()

    assert num_rels == result.shape[0]


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_beta_graph_relationships_to_undirected(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "Node", ["REL", "REL2"])

    if gds.server_version() >= ServerVersion(2, 5, 0):
        with pytest.warns(DeprecationWarning):
            result = gds.beta.graph.relationships.toUndirected(G, "REL", "REL_UNDIRECTED")
    else:
        result = gds.beta.graph.relationships.toUndirected(G, "REL", "REL_UNDIRECTED")

    assert result["relationshipsWritten"] == 6
    assert "REL_UNDIRECTED" in G.relationship_types()


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_graph_relationships_to_undirected(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "Node", ["REL", "REL2"])

    result = gds.graph.relationships.toUndirected(G, "REL", "REL_UNDIRECTED")
    assert result["relationshipsWritten"] == 6
    assert "REL_UNDIRECTED" in G.relationship_types()


def test_graph_writeNodeProperties(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.pageRank.mutate(G, mutateProperty="rank", dampingFactor=0.2, tolerance=0.3)

    with pytest.warns(DeprecationWarning):
        result = gds.graph.writeNodeProperties(G, ["rank"], concurrency=2)
    assert result["propertiesWritten"] == 3


def test_graph_writeRelationship(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.nodeSimilarity.mutate(G, mutateRelationshipType="SIMILAR", mutateProperty="score", similarityCutoff=0)

    with pytest.warns(DeprecationWarning):
        result = gds.graph.writeRelationship(G, "SIMILAR", "score", concurrency=2)
    assert result["relationshipsWritten"] == 2
    assert result["propertiesWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 4, 0))
def test_graph_relationshipProperties_write(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", {"R": {"type": "REL", "properties": ["relX", "relY"]}})

    result = gds.graph.relationshipProperties.write(
        G,
        "R",
        ["relX", "relY"],
    )
    assert result["relationshipsWritten"] == 3
    assert result["propertiesWritten"] == 6


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationship_write(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    gds.nodeSimilarity.mutate(G, mutateRelationshipType="SIMILAR", mutateProperty="score", similarityCutoff=0)

    result = gds.graph.relationship.write(G, "SIMILAR", "score", concurrency=2)
    assert result["relationshipsWritten"] == 2
    assert result["propertiesWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_nodeLabel_write(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    if gds._server_version < ServerVersion(2, 5, 0):
        result = gds.alpha.graph.nodeLabel.write(G, "FilteredNode", nodeFilter="n.x > 1", concurrency=2)
    else:
        result = gds.graph.nodeLabel.write(G, "FilteredNode", nodeFilter="n.x > 1", concurrency=2)

    assert result["nodeLabelsWritten"] == 2


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 3, 0))
def test_graph_nodeLabel_mutate(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")
    if gds._server_version < ServerVersion(2, 5, 0):
        result = gds.alpha.graph.nodeLabel.mutate(G, "FilteredNode", nodeFilter="n.x > 1", concurrency=2)
    else:
        result = gds.graph.nodeLabel.mutate(G, "FilteredNode", nodeFilter="n.x > 1", concurrency=2)

    assert result["nodeLabelsWritten"] == 2


def test_graph_removeNodeProperties_21(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds.graph.removeNodeProperties(G, ["x"], concurrency=2)
    assert result["propertiesRemoved"] == 3


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_removeNodeProperties_22(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.graph.nodeProperties.drop(G, ["x"], concurrency=2)
    assert result["propertiesRemoved"] == 3


@pytest.mark.compatible_with(max_exclusive=ServerVersion(2, 1, 0))
def test_graph_removeNodeProperties_20(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    with pytest.warns(DeprecationWarning):
        result = gds.graph.removeNodeProperties(G, ["x"], ["*"], concurrency=2)
    assert result["propertiesRemoved"] == 3


def test_graph_deleteRelationships(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", ["REL", "REL2"])

    with pytest.warns(DeprecationWarning):
        result = gds.graph.deleteRelationships(G, "REL")
    assert result["deletedRelationships"] == 3


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_relationships_drop(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", ["REL", "REL2"])

    result = gds.graph.relationships.drop(G, "REL")
    assert result["deletedRelationships"] == 3


def test_beta_graph_generate(gds: GraphDataScience) -> None:
    if gds.server_version() >= ServerVersion(2, 5, 0):
        with pytest.warns(DeprecationWarning):
            G, result = gds.beta.graph.generate(GRAPH_NAME, 12, 2)
    else:
        G, result = gds.beta.graph.generate(GRAPH_NAME, 12, 2)

    assert G.node_count() == 12
    assert result["generateMillis"] >= 0


@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 5, 0))
def test_graph_generate(gds: GraphDataScience) -> None:
    G, result = gds.graph.generate(GRAPH_NAME, 12, 2)

    assert G.node_count() == 12
    assert result["generateMillis"] >= 0


@pytest.mark.enterprise
@pytest.mark.compatible_with(min_inclusive=ServerVersion(2, 2, 0))
def test_graph_nodeProperty_stream_via_run_query(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, {"Node": {"properties": "x"}}, "*")

    result = gds.run_cypher(
        (
            f"CALL gds.graph.nodeProperty.stream('{G.name()}', 'x') "
            "YIELD nodeId AS id, propertyValue AS degree RETURN id, degree LIMIT 10"
        )
    )
    assert {e for e in result["degree"]} == {1, 2, 3}


def test_empty_relationships_stream(gds: GraphDataScience) -> None:
    G = gds.graph.construct(GRAPH_NAME, nodes=DataFrame({"nodeId": [0, 1]}))
    gds.nodeSimilarity.filtered.mutate(
        G, mutateRelationshipType="SIMILAR", mutateProperty="score", similarityCutoff=0.99
    )

    assert G.relationship_count() == 0
    assert G.relationship_types()

    result = gds.graph.relationships.stream(G, ["SIMILAR"])
    assert result.empty
