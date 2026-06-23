from graphdatascience.session import AuraGraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec

# KGE predict is only mapped for the Cypher (plugin) surface; Arrow is not mapped yet.
SESSION_UNMAPPED_ENDPOINTS = UNMAPPED_ENDPOINTS | {
    "kge.predict.mutate",
    "kge.predict.stream",
    "kge.predict.write",
    # server_version, license.state and debug.sys_info are only exposed on the Cypher (plugin) surface.
    "server_version",
    "license.state",
    "debug.sys_info",
}


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    assert_api_spec_coverage(AuraGraphDataScience, gds_api_spec, unmapped_endpoints=SESSION_UNMAPPED_ENDPOINTS)
