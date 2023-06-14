import pytest

from .conftest import CollectingQueryRunner
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_simple(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_custom_param_name(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"the_graph": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($the_graph, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_lots_of_whitespace(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds .graph. project\n(\t$graph_name  ,s, t)")

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds .graph. project\n(\t$graph_name  ,s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_existing_params(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    G, _ = gds.graph.cypher.project(
        "g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)", {"graph_name": "g"}
    )

    assert G.name() == "g"

    assert runner.last_params() == {"graph_name": "g"}
    assert runner.last_query() == "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t)"


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_return_not_being_last(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) AS graph")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_no_return(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must contain exactly one `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t)")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_multiple_returns(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the query must end with the `RETURN gds.graph.project\\(\\.\\.\\.\\)` call: .+",
    ):
        gds.graph.cypher.project(
            "g",
            "MATCH (s)-->(t) RETURN gds.graph.project($graph_name, s, t) RETURN gds.graph.project($graph_name, s, t)",
        )


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_non_param_name(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the `graph_name` must use a query parameter, but got `'graph_name'`: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project('graph_name', s, t)")


@pytest.mark.parametrize("server_version", [ServerVersion(2, 4, 0)])
def test_with_existing_but_wrong_param(runner: CollectingQueryRunner, gds: GraphDataScience) -> None:
    with pytest.raises(
        ValueError,
        match="Invalid query, the `gg` parameter must be bound to `g`: .+",
    ):
        gds.graph.cypher.project("g", "MATCH (s)-->(t) RETURN gds.graph.project($gg, s, t)", params={"gg": "h"})
