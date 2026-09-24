from collections import OrderedDict
from unittest import mock

from pandas import DataFrame

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.api.node_embedding.graphsage_runtime_model import GraphSageSupervisedModel
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_supervised_arrow_endpoints import (
    GraphSageSupervisedArrowEndpoints,
)

_TRAIN_ENDPOINT = "v2/embeddings.graphSage.supervised.train"
_PREDICT_ENDPOINT = "v2/embeddings.graphSage.supervised.predict"


def _graph() -> mock.Mock:
    G = mock.Mock()
    G.name.return_value = "g"
    return G


def _endpoints(show_progress: bool = True) -> GraphSageSupervisedArrowEndpoints:
    return GraphSageSupervisedArrowEndpoints(
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
            target_label="Paper",
            target_property="subject",
            epochs=15,
            class_weights=True,
        )

    assert run_summary.call_args.args[0] == _TRAIN_ENDPOINT
    config = run_summary.call_args.args[1]
    assert config["modelName"] == "my-model"
    assert config["featureProperties"] == ["f1", "f2"]
    assert config["targetLabel"] == "Paper"
    assert config["targetProperty"] == "subject"
    assert config["epochs"] == 15
    assert config["classWeights"] is True
    assert config["splitRatios"] == {"TRAIN": 0.6, "TEST": 0.2, "VALID": 0.2}
    assert config["numNeighbors"] == [20, 10]

    assert isinstance(model, GraphSageSupervisedModel)
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
        return_value=DataFrame({"nodeId": [0], "predictedClass": [1], "predictedProbabilities": [[0.2, 0.8]]}),
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


def test_write_overwrites_predicted_class_property() -> None:
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
            write_property="predictedClass",
        )

    assert run_write.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_write.call_args.kwargs["property_overwrites"] == {"predictedClass": "predictedClass"}

    assert result.node_properties_written == 2


def test_write_with_probability_property_overwrites_both() -> None:
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
        endpoints.write(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
            write_property="class",
            predicted_probability_property="probs",
        )

    assert run_write.call_args.kwargs["property_overwrites"] == {
        "predictedClass": "class",
        "predictedProbabilities": "probs",
    }


def test_mutate_single_property() -> None:
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
            mutate_property="predictedClass",
        )

    assert run_mutate.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_mutate.call_args.args[2] == "predictedClass"

    assert result.mutate_millis == 2


def test_mutate_with_probability_property_mutates_both() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_mutate_multiple",
        return_value={
            "computeMillis": 1,
            "configuration": {},
            "mutateMillis": 2,
            "nodePropertiesWritten": 3,
            "preProcessingMillis": 4,
        },
    ) as run_mutate_multiple:
        endpoints.mutate(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
            mutate_property="class",
            predicted_probability_property="probs",
        )

    assert run_mutate_multiple.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_mutate_multiple.call_args.args[2] == OrderedDict(
        [("predictedClass", "class"), ("predictedProbabilities", "probs")]
    )
