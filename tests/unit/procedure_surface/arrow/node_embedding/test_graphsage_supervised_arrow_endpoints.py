from collections import OrderedDict
from unittest import mock

import pytest
from pandas import DataFrame
from pyarrow import ArrowInvalid

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.error.feature_not_enabled import FeatureNotEnabledError
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.graphsage_supervised_model import GraphSageSupervisedModel
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
        return_value={"configuration": {}, "preProcessingMillis": 1, "train_ms": 2},
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
            "predict_ms": 1,
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
    assert run_write.call_args.kwargs["property_overwrites"] == {"predicted_class": "predictedClass"}

    assert result.node_properties_written == 2


def test_write_with_probability_property_overwrites_both() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_write",
        return_value={
            "predict_ms": 1,
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
        "predicted_class": "class",
        "predicted_probabilities": "probs",
    }


# A session without the python-runtime rejects the action with this invalid-argument error.
_UNSUPPORTED_ACTION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: "
    "Unsupported action: v2/embeddings.graphSage.supervised.predict. Supported: ['v2/embeddings.fastrp']"
)

# An unrelated invalid-argument error (config validation) that must keep propagating unchanged.
_CONFIG_VALIDATION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: Must specify featureProperties"
)


def _compute(endpoints: GraphSageSupervisedArrowEndpoints) -> None:
    endpoints.compute(
        G=_graph(),
        model_name="my-model",
        feature_properties=["f1"],
    )


def test_compute_returns_job_handle() -> None:
    endpoints = _endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job",
        return_value="job-123",
    ) as run_job:
        handle = endpoints.compute(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1"],
        )

    assert isinstance(handle, JobHandle)
    assert handle.job_id() == "job-123"
    assert run_job.call_args.args[1] == _PREDICT_ENDPOINT
    config = run_job.call_args.args[2]
    assert config["modelName"] == "my-model"
    assert config["featureProperties"] == ["f1"]
    assert "targetProperty" not in config
    assert "epochs" not in config


def test_compute_translates_unsupported_action_to_feature_not_enabled() -> None:
    endpoints = _endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job",
        side_effect=_UNSUPPORTED_ACTION_ERROR,
    ):
        with pytest.raises(FeatureNotEnabledError, match="not enabled for this session"):
            _compute(endpoints)


def test_compute_keeps_original_error_as_cause() -> None:
    endpoints = _endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job",
        side_effect=_UNSUPPORTED_ACTION_ERROR,
    ):
        with pytest.raises(FeatureNotEnabledError) as exc_info:
            _compute(endpoints)

    assert exc_info.value.__cause__ is _UNSUPPORTED_ACTION_ERROR


def test_compute_unrelated_invalid_argument_error_is_not_translated() -> None:
    endpoints = _endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job",
        side_effect=_CONFIG_VALIDATION_ERROR,
    ):
        with pytest.raises(ArrowInvalid, match="Must specify featureProperties"):
            _compute(endpoints)


def test_mutate_single_property() -> None:
    endpoints = _endpoints()

    with mock.patch.object(
        endpoints._node_property_endpoints,
        "run_job_and_mutate",
        return_value={
            "predict_ms": 1,
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
            "predict_ms": 1,
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
        [("predicted_class", "class"), ("predicted_probabilities", "probs")]
    )
