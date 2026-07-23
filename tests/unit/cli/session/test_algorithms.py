from unittest.mock import MagicMock

import pytest
from gds_cli.session.algorithms import (
    compute_algorithm,
    input_property_references,
    resolve_endpoint,
    to_snake_case,
    to_snake_params,
)
from gds_cli.session.config import ComputeSpec


def test_resolve_endpoint_maps_canonical_name() -> None:
    gds = MagicMock()

    endpoint = resolve_endpoint(gds, "PageRank")

    assert endpoint is gds.page_rank


def test_resolve_endpoint_maps_fastpath() -> None:
    gds = MagicMock()

    endpoint = resolve_endpoint(gds, "fastpath")

    assert endpoint is gds.fast_path


def test_resolve_endpoint_unknown_algorithm_raises() -> None:
    gds = MagicMock()

    with pytest.raises(ValueError, match="Unknown algorithm"):
        resolve_endpoint(gds, "not-a-real-algorithm")


def test_resolve_endpoint_missing_attr_raises() -> None:
    gds = object()  # no page_rank attribute

    with pytest.raises(ValueError, match="has no endpoint"):
        resolve_endpoint(gds, "pageRank")  # type: ignore[arg-type]


def test_to_snake_case() -> None:
    assert to_snake_case("maxIterations") == "max_iterations"
    assert to_snake_case("dampingFactor") == "damping_factor"
    assert to_snake_case("useWassermanFaust") == "use_wasserman_faust"
    assert to_snake_case("tolerance") == "tolerance"


def test_to_snake_params_converts_keys_only() -> None:
    params = {"maxIterations": 20, "featureProperties": ["ageGroup"]}

    assert to_snake_params(params) == {"max_iterations": 20, "feature_properties": ["ageGroup"]}


def test_compute_algorithm_calls_compute_with_snake_case_params() -> None:
    gds = MagicMock()
    graph = MagicMock()
    spec = ComputeSpec(compute="pageRank", config={"resultProperty": "pagerank", "maxIterations": 5})

    handle = compute_algorithm(gds, graph, spec)

    gds.page_rank.compute.assert_called_once_with(graph, max_iterations=5)
    assert handle is gds.page_rank.compute.return_value


def test_compute_algorithm_excludes_result_property_from_params() -> None:
    gds = MagicMock()
    graph = MagicMock()
    spec = ComputeSpec(compute="louvain", config={"resultProperty": "community"})

    compute_algorithm(gds, graph, spec)

    gds.louvain.compute.assert_called_once_with(graph)


def test_input_property_references_reads_registered_keys() -> None:
    # fastRP reads node properties from featureProperties (a list).
    assert input_property_references("fastRP", {"featureProperties": ["pagerank", "age"]}) == {"pagerank", "age"}
    # labelPropagation reads seedProperty + nodeWeightProperty (strings).
    assert input_property_references("labelPropagation", {"seedProperty": "seed", "nodeWeightProperty": "w"}) == {
        "seed",
        "w",
    }


def test_input_property_references_empty_for_algorithms_without_node_inputs() -> None:
    # pageRank's relationshipWeightProperty is a relationship property, not a node input.
    assert input_property_references("pageRank", {"relationshipWeightProperty": "weight"}) == set()


def test_compute_algorithm_errors_when_endpoint_has_no_compute() -> None:
    gds = MagicMock()
    gds.louvain = object()  # endpoint lacking a `compute` attribute
    graph = MagicMock()
    spec = ComputeSpec(compute="louvain", config={"resultProperty": "community"})

    with pytest.raises(ValueError, match="has no `compute` method"):
        compute_algorithm(gds, graph, spec)
