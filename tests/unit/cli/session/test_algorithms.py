from unittest.mock import MagicMock

import pytest
from gds_cli.session.algorithms import resolve_endpoint, run_algorithm
from gds_cli.session.config import AlgorithmConfig


def test_resolve_endpoint_maps_canonical_name() -> None:
    gds = MagicMock()

    endpoint = resolve_endpoint(gds, "PageRank")

    assert endpoint is gds.page_rank


def test_resolve_endpoint_unknown_algorithm_raises() -> None:
    gds = MagicMock()

    with pytest.raises(ValueError, match="Unknown algorithm"):
        resolve_endpoint(gds, "not-a-real-algorithm")


def test_resolve_endpoint_missing_attr_raises() -> None:
    gds = object()  # no page_rank attribute

    with pytest.raises(ValueError, match="has no endpoint"):
        resolve_endpoint(gds, "pageRank")  # type: ignore[arg-type]


def test_run_algorithm_mutate_mode_calls_mutate() -> None:
    gds = MagicMock()
    graph = MagicMock()
    algo = AlgorithmConfig(
        name="louvain",
        graph_name="social",
        mode="mutate",
        mutate_property="community",
        parameters={"maxIterations": 5},
    )

    run_algorithm(gds, graph, algo)

    gds.louvain.mutate.assert_called_once_with(graph, mutate_property="community", maxIterations=5)


def test_run_algorithm_write_mode_calls_write() -> None:
    gds = MagicMock()
    graph = MagicMock()
    algo = AlgorithmConfig(name="pageRank", graph_name="pages", mode="write", write_property="rank")

    run_algorithm(gds, graph, algo)

    gds.page_rank.write.assert_called_once_with(graph, write_property="rank")
