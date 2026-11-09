from graphdatascience.session import AuraGraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec
from tests.integration.procedure_surface.session_api_spec import SessionApiSpec
from tests.integration.procedure_surface.session_api_spec_coverage_test_helper import (
    assert_session_api_spec_coverage,
)

# KGE predict is only mapped for the Cypher (plugin) surface; Arrow is not mapped yet.
SESSION_UNMAPPED_ENDPOINTS = UNMAPPED_ENDPOINTS | {
    "kge.predict.mutate",  # not mapped on the Server yet
    "kge.predict.stream",  # not mapped on the Server yet
    "kge.predict.write",  # not mapped on the Server yet
    "server_version",  # AGA is versionless
    "is_licensed",  # AGA is always licensed
    "license.state",  # AGA is always licensed
    "debug.sys_info",  # AGA cannot spill internal runtime details
    "memory.summary",  # plugin only; no Arrow/Session equivalent
    "memory.list",  # plugin only; no Arrow/Session equivalent
}


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec], session_api_spec: SessionApiSpec) -> None:
    # Algorithms the GDS Sessions API spec describes are validated against it (including
    # parameter types and defaults). Everything else the session surface exposes (graph and
    # model catalog, pipelines, ...) is still validated against the plugin API spec.
    job_spec_endpoints = {endpoint.name for endpoint in session_api_spec.endpoints()}

    assert_api_spec_coverage(
        AuraGraphDataScience,
        gds_api_spec,
        unmapped_endpoints=SESSION_UNMAPPED_ENDPOINTS,
        include_arrow_only=True,
        exclude_endpoints=job_spec_endpoints,
    )
    assert_session_api_spec_coverage(AuraGraphDataScience, session_api_spec)
