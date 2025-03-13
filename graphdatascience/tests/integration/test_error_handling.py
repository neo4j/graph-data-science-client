from typing import Generator

import pytest
from neo4j import Driver, GraphDatabase

from graphdatascience import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion
from graphdatascience.tests.integration.conftest import AUTH, URI, is_neo4j_44

GRAPH_NAME = "g"


@pytest.fixture(autouse=True)
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


@pytest.fixture
def warning_driver() -> Generator[Driver, None, None]:
    driver = GraphDatabase.driver(URI, auth=AUTH, warn_notification_severity="WARNING")

    yield driver

    driver.close()


def test_bogus_algo(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(SyntaxError, match="There is no 'gds.bogusAlgoWithLongName.stream' to call$"):
        gds.bogusAlgoWithLongName.stream(G)


def test_suggest_correct_algo_endpoint(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")

    # Similar enough
    with pytest.raises(
        SyntaxError, match="There is no 'gds.pagerank.stream' to call. Did you mean 'gds.pageRank.stream'?"
    ):
        gds.pagerank.stream(G)

    # Too different
    with pytest.raises(SyntaxError, match="There is no 'gds.peggyRankerTroll.stream' to call$"):
        gds.peggyRankerTroll.stream(G)


def test_suggest_with_wrong_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.bersion' to call"):
        gds.bersion()


def test_no_suggestive_cypher_exception(gds: GraphDataScience) -> None:
    G, _ = gds.graph.project(GRAPH_NAME, "*", "*")
    with pytest.raises(
        Exception,
        match="There is no procedure with the name `gds.pagerank.stream` registered for this database instance",
    ):
        gds.run_cypher(f"CALL gds.pagerank.stream('{G.name()}')")


def test_acknowledge_ignored_server_call(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="The call 'gds.util.NaN' is a valid GDS server endpoint, but does not have a corresponding Python method",
    ):
        gds.util.NaN()  # type: ignore


def test_calling_gds(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds' to call"):
        gds()


def test_nonexisting_direct_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.bogus' to call"):
        gds.bogus()


def test_nonexisting_indirect_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.bogus.thing' to call"):
        gds.bogus.thing()


def test_calling_graph(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph' to call"):
        gds.graph("hello")


def test_nonexisting_graph_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph.bogus' to call"):
        gds.graph.bogus("hello")  # type: ignore


def test_nonexisting_graph_export_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph.export.bogusBananaStuff' to call"):
        gds.graph.export.bogusBananaStuff("hello")  # type: ignore


def test_nonexisting_graph_project_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.graph.project.bogus' to call"):
        gds.graph.project.bogus("there")  # type: ignore


def test_calling_model(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.model' to call"):
        gds.model(42, 1337)


def test_nonexisting_model_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.model.bogus' to call"):
        gds.model.bogus(42, 1337)  # type: ignore


def test_calling_pipeline(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.pipeline' to call"):
        gds.pipeline(42, 1337)


def test_nonexisting_pipeline_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.pipeline.bogus' to call"):
        gds.pipeline.bogus(42, 1337)  # type: ignore


def test_calling_linkPrediction(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.pipeline.linkPrediction' to call"):
        gds.beta.pipeline.linkPrediction()


def test_nonexisting_linkPrediction_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.pipeline.linkPrediction.whoops' to call"):
        gds.beta.pipeline.linkPrediction.whoops()  # type: ignore


def test_calling_nodeClassification(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.beta.pipeline.nodeClassification' to call",
    ):
        gds.beta.pipeline.nodeClassification(13.37)


def test_nonexisting_nodeClassification_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.beta.pipeline.nodeClassification.whoops' to call"):
        gds.beta.pipeline.nodeClassification.whoops(13.37)  # type: ignore


def test_calling_linkprediction(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.alpha.linkprediction' to call",
    ):
        gds.alpha.linkprediction(1, 2, direction="REVERSE")


def test_nonexisting_linkprediction_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.alpha.linkprediction.adamicFoobar' to call"):
        gds.alpha.linkprediction.adamicFoobar(1, 2, direction="REVERSE")  # type: ignore


def test_calling_debug(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.debug' to call"):
        gds.debug()


def test_nonexisting_debug_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.debug.sniffDumpo' to call?"):
        gds.debug.sniffDumpo()  # type: ignore


def test_calling_util(gds: GraphDataScience) -> None:
    with pytest.raises(
        SyntaxError,
        match="There is no 'gds.util' to call",
    ):
        gds.util()


def test_nonexisting_util_endpoint(gds: GraphDataScience) -> None:
    with pytest.raises(SyntaxError, match="There is no 'gds.util.askNodezzzzz' to call"):
        gds.util.askNodezzzzz()  # type: ignore


def test_auto_completion_false_positives(gds: GraphDataScience) -> None:
    # Without `graph` prefix
    with pytest.raises(SyntaxError, match="There is no 'gds.toUndirected' to call"):
        gds.toUndirected()


def test_forward_server_side_warning(gds: GraphDataScience) -> None:
    if is_neo4j_44(gds):
        return

    with pytest.raises(Warning, match="The query used a deprecated function: `id`."):
        gds.run_cypher("MATCH (n) RETURN id(n)")


@pytest.mark.filterwarnings("ignore: notification warnings are a preview feature")
@pytest.mark.compatible_with_db_driver(min_inclusive=ServerVersion(5, 21, 0))
def test_forward_driver_configured_warning(warning_driver: Driver) -> None:
    gds = GraphDataScience(warning_driver)

    if is_neo4j_44(gds):
        return

    with pytest.raises(Warning, match="The query used a deprecated function: `id`."):
        gds.run_cypher("MATCH (n) RETURN id(n)")


def test_filter_out_client_related__warning(gds: GraphDataScience) -> None:
    gds.graph.drop("g", failIfMissing=False)


@pytest.mark.filterwarnings("ignore: notification warnings are a preview feature")
@pytest.mark.compatible_with_db_driver(min_inclusive=ServerVersion(5, 21, 0))
def test_filter_out_client_related__driver_configured_warning(warning_driver: Driver) -> None:
    gds = GraphDataScience(warning_driver)
    gds.graph.drop("g", failIfMissing=False)
