import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph.graph_cypher_runner import GraphCypherRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_simple(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g", "don't squeeze": "me now"}]))

    G, _ = gds.graph.cypher.project("MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_fstring(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g", "don't squeeze": "me now"}]))

    graph_name = "g"
    G, _ = gds.graph.cypher.project(f"MATCH (s)-->(t) RETURN gds.graph.project('{graph_name}', s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_expression(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "gg", "don't squeeze": "me now"}]))

    G, _ = gds.graph.cypher.project("WITH 'g' AS suffix MATCH (s)-->(t) RETURN gds.graph.project('g' + suffix, s, t)")

    assert G.name() == "gg"

    assert runner.last_params() == {}
    assert runner.last_query() == "WITH 'g' AS suffix MATCH (s)-->(t) RETURN gds.graph.project('g' + suffix, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_parameter(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g", "don't squeeze": "me now"}]))

    G, _ = gds.graph.cypher.project("MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)", the_graph="g")

    assert G.name() == "g"

    assert runner.last_params() == {"the_graph": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_lots_of_whitespace(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g", "don't squeeze": "me now"}]))

    G, _ = gds.graph.cypher.project("MATCH (s)-->(t) RETURN gds .graph. project\n(\t'g'  ,s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds .graph. project\n(\t'g'  ,s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_extracting_graph_name(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "the graph", "don't squeeze": "me now"}]))

    G, _ = gds.graph.cypher.project("MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)")

    assert G.name() == "the graph"

    assert runner.last_params() == {}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_return_not_being_last(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) AS graph")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_no_return(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must contain exactly one `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("MATCH (s)-->(t)")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_multiple_returns(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project(
            "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) RETURN gds.graph.project($graph_name, s, t)",
        )


@pytest.mark.parametrize(
    "query",
    [
        "MATCH (s)-->(t) RETURN gds.graph.project('g', s, t)",
        "MATCH (s)-->(t) RETURN gds.graph.project('g' + foo, s, t)",
        "MATCH (s)-->(t) RETURN gds.graph.project('g' + CALL foo(bar), s, t)",
        "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)",
        "MATCH (s)-->(t) RETURN gds.graph.project($the_gg, s, t)",
        "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t, { property: labels(s) })",
    ],
)
def test_verify_query_ends_with_return_clause(query: str) -> None:
    GraphCypherRunner._verify_query_ends_with_return_clause("gds.graph.project", query)


@pytest.mark.parametrize(
    "query, error",
    [
        (
            "MATCH (s)-->(t) WITH gds.graph.project($graph_name, s, t)",
            "Invalid query, the query must contain exactly one `RETURN gds\\.graph\\.project\\(...\\)` call: .+",
        ),
        (
            "MATCH (s)-->(t) WITH gds.graph.project($graph_name, s, t",
            "Invalid query, the query must contain exactly one `RETURN gds\\.graph\\.project\\(...\\)` call: .+",
        ),
        (
            "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) AS g",
            "Invalid query, the query must end with the `RETURN gds\\.graph\\.project\\(...\\)` call: .+",
        ),
    ],
)
def test_find_return_clause_errors(query: str, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        GraphCypherRunner._verify_query_ends_with_return_clause("gds.graph.project", query)
