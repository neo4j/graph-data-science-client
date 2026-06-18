from unittest import mock

import pandas as pd
import pytest

from graphdatascience.error.standalone_session_error import NotAvailableInStandaloneSessions
from graphdatascience.procedure_surface.arrow.util_arrow_endpoints import UtilArrowEndpoints
from graphdatascience.query_runner.query_mode import QueryMode
from tests.unit.conftest import DEFAULT_SERVER_VERSION, CollectingQueryRunner


def _runner() -> CollectingQueryRunner:
    return CollectingQueryRunner(DEFAULT_SERVER_VERSION)


def test_as_node_runs_db_cypher() -> None:
    query_runner = _runner()
    query_runner.add__mock_result("MATCH (n) WHERE id(n) = $nodeId", pd.DataFrame({"n": ["node-1"]}))
    util = UtilArrowEndpoints(query_runner)

    assert util.as_node(1) == "node-1"
    assert query_runner.last_query() == "MATCH (n) WHERE id(n) = $nodeId RETURN n"
    assert query_runner.last_params() == {"nodeId": 1}
    run_args = query_runner.last_run_args()
    assert run_args["mode"] == QueryMode.READ
    assert run_args["retryable"] is True


def test_as_nodes_runs_db_cypher() -> None:
    query_runner = _runner()
    query_runner.add__mock_result("MATCH (n) WHERE id(n) IN $nodeIds", pd.DataFrame({"nodes": [["node-1", "node-2"]]}))
    util = UtilArrowEndpoints(query_runner)

    assert util.as_nodes([1, 2]) == ["node-1", "node-2"]
    assert query_runner.last_query() == "MATCH (n) WHERE id(n) IN $nodeIds RETURN collect(n)"
    assert query_runner.last_params() == {"nodeIds": [1, 2]}


def test_one_hot_encoding_is_client_side() -> None:
    query_runner = _runner()
    util = UtilArrowEndpoints(query_runner)

    assert util.one_hot_encoding(["a", "b", "c"], ["b"]) == [0, 1, 0]
    # Computed client-side: no query is issued against the database.
    assert query_runner.queries == []


def test_one_hot_encoding_works_without_database() -> None:
    # Standalone session (no DB) is fine: one_hot_encoding does not touch the database.
    util = UtilArrowEndpoints(None)

    assert util.one_hot_encoding(["a", "b", "c"], ["a", "c"]) == [1, 0, 1]


def test_node_property_not_supported() -> None:
    util = UtilArrowEndpoints(_runner())
    graph = mock.Mock()
    graph.name.return_value = "g"

    with pytest.raises(NotImplementedError, match="not available in AGA sessions"):
        util.node_property(graph, 1, "rank")


def test_standalone_session_raises() -> None:
    util = UtilArrowEndpoints(None)

    with pytest.raises(NotAvailableInStandaloneSessions):
        util.as_node(1)
    with pytest.raises(NotAvailableInStandaloneSessions):
        util.as_nodes([1])
