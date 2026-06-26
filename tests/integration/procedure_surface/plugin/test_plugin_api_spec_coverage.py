from graphdatascience import GraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    assert_api_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec


def test_plugin_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    assert_api_spec_coverage(GraphDataScience, gds_api_spec, include_arrow_only=False)
