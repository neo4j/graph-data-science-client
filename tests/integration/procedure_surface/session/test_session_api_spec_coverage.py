from graphdatascience.session import AuraGraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec

# KGE predict is only mapped for the Cypher (plugin) surface; Arrow is not mapped yet.
SESSION_UNMAPPED_ENDPOINTS = UNMAPPED_ENDPOINTS | {
    "kge.predict.mutate",  # not mapped on the Server yet
    "kge.predict.stream",  # not mapped on the Server yet
    "kge.predict.write",  # not mapped on the Server yet
    "server_version",  # AGA is versionless
    "is_licensed",  # AGA is always licensed
    "license.state",  # AGA is always licensed
    "debug.sys_info",  # AGA cannot spill internal runtime details
}


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    assert_api_spec_coverage(
        AuraGraphDataScience,
        gds_api_spec,
        unmapped_endpoints=SESSION_UNMAPPED_ENDPOINTS,
        include_arrow_only=True,
    )
