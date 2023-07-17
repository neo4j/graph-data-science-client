import pytest
from pandas import DataFrame

from .conftest import CollectingQueryRunner
from graphdatascience.graph.graph_cypher_runner import GraphCypherRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_simple(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g"}]))

    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_custom_param_name(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g"}]))

    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"the_graph": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_lots_of_whitespace(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g"}]))

    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds .graph. project\n(\t$graph_name  ,s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds .graph. project\n(\t$graph_name  ,s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_existing_params(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "g"}]))

    G, _ = gds.graph.cypher.project(
        "g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)", {"graph_name": "g"}
    )

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_extracting_graph_name(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    runner.set__mock_result(DataFrame([{"graphName": "the graph"}]))

    G, res = gds.graph.cypher.project(
        "g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)", params={"graph_name": "g"}
    )

    assert G.name() == "the graph"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_return_not_being_last(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) AS graph")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_no_return(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must contain exactly one `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t)")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_multiple_returns(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project(
            "g",
            "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) RETURN gds.graph.project($graph_name, s, t)",
        )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_non_param_name(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the `graph_name` must use a query parameter, but got `'graph_name'`: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project('graph_name', s, t)")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_existing_but_wrong_param(gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the `gg` parameter must be bound to `g`: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($gg, s, t)", params={"gg": "h"})


@pytest.mark.parametrize(
    "query, expected",
    [
        ("MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)", "$graph_name"),
        ("MATCH (s)-->(t) RETURN gds.graph.project($the_gg, s, t)", "$the_gg"),
        ("MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t, { property: labels(s) })", "$graph_name"),
    ],
)
def test_find_return_clause_graph_name(query: str, expected: str) -> None:
    actual = GraphCypherRunner._find_return_clause_graph_name("gds.graph.project", query)

    assert actual == expected


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
        GraphCypherRunner._find_return_clause_graph_name("gds.graph.project", query)
