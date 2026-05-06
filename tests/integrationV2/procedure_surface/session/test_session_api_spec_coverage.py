from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from tests.integrationV2.procedure_surface.api_spec_coverage_test_helper import (
    assert_api_spec_coverage,
)
from tests.integrationV2.procedure_surface.gds_api_spec import EndpointWithModesSpec


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    endpoints = SessionV2Endpoints(
        mock.Mock(spec=AuthenticatedArrowClient),
        db_client=None,
        show_progress=False,
    )

    assert_api_spec_coverage(endpoints, gds_api_spec)
