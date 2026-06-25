import warnings
from unittest import mock

import pytest
from pyarrow import ArrowInvalid

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.node_embedding.fastpath_arrow_endpoints import (
    FastPathArrowEndpoints,
    FeatureNotEnabledError,
)

# A session without FastPath rejects the action with this invalid-argument error.
_UNSUPPORTED_ACTION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: "
    "Unsupported action: v2/embeddings.fastPath. Supported: ['v2/embeddings.fastrp']"
)

# An unrelated invalid-argument error (config validation) that must keep propagating unchanged.
_CONFIG_VALIDATION_ERROR = ArrowInvalid(
    "Flight returned invalid argument error, with message: Must specify either outputTime or outputTimeProperty"
)


def _fastpath_endpoints() -> FastPathArrowEndpoints:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        return FastPathArrowEndpoints(mock.Mock(spec=AuthenticatedArrowClient))


def _call(endpoints: FastPathArrowEndpoints) -> None:
    endpoints.stream(
        G=mock.Mock(),
        base_node_label="Base",
        event_node_label="Event",
        dimension=16,
        max_elapsed_time=10,
        num_elapsed_times=4,
    )


def test_unsupported_action_is_translated_to_feature_not_enabled() -> None:
    endpoints = _fastpath_endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
        side_effect=_UNSUPPORTED_ACTION_ERROR,
    ):
        with pytest.raises(FeatureNotEnabledError, match="not enabled for this session"):
            _call(endpoints)


def test_unsupported_action_keeps_original_error_as_cause() -> None:
    endpoints = _fastpath_endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
        side_effect=_UNSUPPORTED_ACTION_ERROR,
    ):
        with pytest.raises(FeatureNotEnabledError) as exc_info:
            _call(endpoints)

    assert exc_info.value.__cause__ is _UNSUPPORTED_ACTION_ERROR


def test_unrelated_invalid_argument_error_is_not_translated() -> None:
    endpoints = _fastpath_endpoints()

    with mock.patch(
        "graphdatascience.procedure_surface.arrow.endpoints_helper_base.JobClient.run_job_and_wait",
        side_effect=_CONFIG_VALIDATION_ERROR,
    ):
        with pytest.raises(ArrowInvalid, match="Must specify either outputTime"):
            _call(endpoints)
