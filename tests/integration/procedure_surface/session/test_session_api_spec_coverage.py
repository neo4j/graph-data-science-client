from graphdatascience.session import AuraGraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    assert_api_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    assert_api_spec_coverage(AuraGraphDataScience, gds_api_spec)
