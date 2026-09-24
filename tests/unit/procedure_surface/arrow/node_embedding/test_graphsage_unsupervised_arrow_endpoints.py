from unittest import mock

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_model import (
    GraphSageUnsupervisedModel,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_unsupervised_arrow_endpoints import (
    GraphSageUnsupervisedArrowEndpoints,
)

_TRAIN_ENDPOINT = "v2/embeddings.graphSage.unsupervised.train"
_PREDICT_ENDPOINT = "v2/embeddings.graphSage.unsupervised.predict"


def _graph() -> mock.Mock:
    G = mock.Mock()
    G.name.return_value = "g"
    return G


def _endpoints(show_progress: bool = True) -> GraphSageUnsupervisedArrowEndpoints:
    return GraphSageUnsupervisedArrowEndpoints(
        arrow_client=mock.Mock(spec=AuthenticatedArrowClient), write_protocol=None, show_progress=show_progress
    )


def test_train_runs_against_train_endpoint() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_get_summary",
        return_value={"configuration": {}, "preProcessingMillis": 1, "trainMillis": 2},
    ) as run_summary:
        model, result = endpoints.train(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1", "f2"],
            epochs=15,
            num_walks=7,
        )

    assert run_summary.call_args.args[0] == _TRAIN_ENDPOINT
    config = run_summary.call_args.args[1]
    assert config["modelName"] == "my-model"
    assert config["featureProperties"] == ["f1", "f2"]
    assert config["epochs"] == 15
    assert config["numWalks"] == 7
    assert config["dropout"] == 0.1
    assert config["numNeighbors"] == [20, 10]

    assert isinstance(model, GraphSageUnsupervisedModel)
    assert model.name() == "my-model"
    assert result.train_millis == 2
    assert result.pre_processing_millis == 1


def test_train_forwards_show_progress() -> None:
    assert _endpoints(show_progress=False)._show_progress is False
    assert _endpoints()._show_progress is True


def test_stream_runs_against_predict_endpoint() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_stream",
        return_value=DataFrame({"nodeId": [0], "embedding": [[0.1, 0.2]]}),
    ) as run_stream:
        result = endpoints.stream(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
        )

    assert run_stream.call_args.args[0] == _PREDICT_ENDPOINT
    config = run_stream.call_args.args[2]
    assert config["modelName"] == "my-model"
    assert config["featureProperties"] == ["f1"]
    assert "epochs" not in config

    assert isinstance(result, DataFrame)


def test_write_runs_against_predict_endpoint() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_write",
        return_value={
            "computeMillis": 1,
            "configuration": {},
            "nodePropertiesWritten": 2,
            "preProcessingMillis": 3,
            "writeMillis": 4,
        },
    ) as run_write:
        result = endpoints.write(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
            write_property="embedding",
        )

    assert run_write.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_write.call_args.kwargs["property_overwrites"] == "embedding"

    assert result.node_properties_written == 2
    assert result.write_millis == 4


def test_mutate_runs_against_predict_endpoint() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_mutate",
        return_value={
            "computeMillis": 1,
            "configuration": {},
            "mutateMillis": 2,
            "nodePropertiesWritten": 3,
            "preProcessingMillis": 4,
        },
    ) as run_mutate:
        result = endpoints.mutate(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
            mutate_property="embedding",
        )

    assert run_mutate.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_mutate.call_args.args[2] == "embedding"

    assert result.mutate_millis == 2
    assert result.node_properties_written == 3
