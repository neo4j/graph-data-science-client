from typing import Any
from unittest import mock

import pytest
from pandas import DataFrame
from pyarrow import ArrowInvalid

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.error.feature_not_enabled import FeatureNotEnabledError
from graphdatascience.procedure_surface.api.job_handle import JobHandle
from graphdatascience.procedure_surface.api.node_embedding.graphsage_unsupervised_model import (
    GraphSageUnsupervisedModel,
)
from graphdatascience.procedure_surface.arrow.node_embedding.graphsage_unsupervised_arrow_endpoints import (
    GraphSageUnsupervisedArrowEndpoints,
)

_TRAIN_ENDPOINT = "v2/embeddings.graphSage.unsupervised.train"
_PREDICT_ENDPOINT = "v2/embeddings.graphSage.unsupervised.predict"

OMISSION_TABLE = [
    ({}, False),
    ({"gpu": False}, False),
    ({"gpu": True}, True),
]
OMISSION_IDS = ["default_omits_gpu", "gpu_false_omits_gpu", "gpu_true_sets_gpu_config"]


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
        return_value={"configuration": {}, "preProcessingMillis": 1, "train_ms": 2},
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


def _train_config(gpu_kwargs: dict[str, Any]) -> dict[str, Any]:
    endpoints = _endpoints()

    with (
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
            return_value="job-1",
        ) as run_job_and_wait,
        mock.patch(
            "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.get_summary",
            return_value={"configuration": {}, "preProcessingMillis": 1, "train_ms": 2},
        ),
    ):
        endpoints.train(
            G=_graph(),
            model_name="my-model",
            feature_properties=["f1", "f2"],
            epochs=1,
            **gpu_kwargs,
        )

    assert run_job_and_wait.call_args.args[1] == _TRAIN_ENDPOINT
    config: dict[str, Any] = run_job_and_wait.call_args.args[2]
    return config


@pytest.mark.parametrize(("gpu_kwargs", "gpu_in_config"), OMISSION_TABLE, ids=OMISSION_IDS)
def test_train_gpu_is_omitted_unless_truthy(gpu_kwargs: dict[str, Any], gpu_in_config: bool) -> None:
    config = _train_config(gpu_kwargs)

    assert ("gpuConfig" in config) is gpu_in_config
    if gpu_in_config:
        assert config["gpuConfig"] == {"enabled": True}


def _run_config(run_mode: str, gpu_kwargs: dict[str, Any]) -> dict[str, Any]:
    """Run the given predict mode with `gpu_kwargs` and return the arrow job config sent to JobClient."""
    endpoints = _endpoints()
    job_client = "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient"

    if run_mode == "compute":
        with mock.patch(f"{job_client}.run_job", return_value="job-1") as run:
            endpoints.compute(G=_graph(), model_name="my-model", feature_properties=["f1"], **gpu_kwargs)
        config: dict[str, Any] = run.call_args.args[2]
    elif run_mode == "stream":
        with (
            mock.patch(f"{job_client}.run_job_and_wait", return_value="job-1") as run,
            mock.patch(f"{job_client}.stream_results", return_value=DataFrame({"nodeId": [0]})),
        ):
            endpoints.stream(G=_graph(), model_name="my-model", feature_properties=["f1"], **gpu_kwargs)
        config = run.call_args.args[2]
    elif run_mode == "write":
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
        ) as run:
            endpoints.write(
                G=_graph(),
                model_name="my-model",
                feature_properties=["f1"],
                write_property="embedding",
                **gpu_kwargs,
            )
        config = run.call_args.args[2]
    else:
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
        ) as run:
            endpoints.mutate(
                G=_graph(),
                model_name="my-model",
                feature_properties=["f1"],
                mutate_property="embedding",
                **gpu_kwargs,
            )
        config = run.call_args.args[1]

    return config


@pytest.mark.parametrize("run_mode", ["compute", "stream", "write", "mutate"])
@pytest.mark.parametrize(("gpu_kwargs", "gpu_in_config"), OMISSION_TABLE, ids=OMISSION_IDS)
def test_predict_gpu_is_omitted_unless_truthy(run_mode: str, gpu_kwargs: dict[str, Any], gpu_in_config: bool) -> None:
    config = _run_config(run_mode, gpu_kwargs)

    assert ("gpuConfig" in config) is gpu_in_config
    if gpu_in_config:
        assert config["gpuConfig"] == {"enabled": True}


# A session without the python-runtime rejects the action with this invalid-argument error.
_UNSUPPORTED_ACTION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: "
    "Unsupported action: v2/embeddings.graphSage.unsupervised.predict. Supported: ['v2/embeddings.fastrp']"
)

# An unrelated invalid-argument error (config validation) that must keep propagating unchanged.
_CONFIG_VALIDATION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: Must specify featureProperties"
)


def _compute(endpoints: GraphSageUnsupervisedArrowEndpoints) -> None:
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
            mutate_property="embedding",
        )

    assert run_mutate.call_args.args[0] == _PREDICT_ENDPOINT
    assert run_mutate.call_args.args[2] == "embedding"

    assert result.mutate_millis == 2
    assert result.node_properties_written == 3
