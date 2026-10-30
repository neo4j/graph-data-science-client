from unittest import mock

from graphdatascience.procedure_surface.api.default_values import ALL_LABELS, ALL_TYPES
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_session_endpoints import (
    GraphSageSessionEndpoints,
)


def _facade(predict_endpoints: mock.Mock) -> GraphSageSessionEndpoints:
    return GraphSageSessionEndpoints(
        train_endpoints=mock.Mock(),
        predict_endpoints=predict_endpoints,
        catalog_endpoints=mock.Mock(),
        unsupervised_endpoints=mock.Mock(),
        supervised_endpoints=mock.Mock(),
    )


def test_compute_delegates_to_predict_endpoints() -> None:
    predict_endpoints = mock.Mock()
    handle = mock.Mock()
    predict_endpoints.compute.return_value = handle

    facade = _facade(predict_endpoints)
    G = mock.Mock()

    result = facade.compute(G, "my-model", batch_size=10, concurrency=4)

    assert result is handle
    predict_endpoints.compute.assert_called_once_with(
        G,
        "my-model",
        relationship_types=ALL_TYPES,
        node_labels=ALL_LABELS,
        username=None,
        log_progress=True,
        sudo=False,
        concurrency=4,
        job_id=None,
        batch_size=10,
        result_as_vector=False,
    )


def test_compute_forwards_result_as_vector() -> None:
    predict_endpoints = mock.Mock()
    predict_endpoints.compute.return_value = mock.Mock()

    facade = _facade(predict_endpoints)

    facade.compute(mock.Mock(), "my-model", result_as_vector=True)

    assert predict_endpoints.compute.call_args.kwargs["result_as_vector"] is True
