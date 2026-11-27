from graphdatascience.session import AuraGraphDataScience
from tests.integration.procedure_surface.api_spec import ApiSpec
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
    pythonic_endpoint_name,
)
from tests.integration.procedure_surface.endpoint_spec_coverage_test_helper import (
    SESSION_CLIENT_ONLY_PARAMETERS,
    assert_endpoint_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec

# Graph catalog endpoints the session surface does not map yet. Drop entries one at a
# time as the client grows them.
SESSION_UNMAPPED_GRAPH_CATALOG_ENDPOINTS = {
    "graph.size_of",  # the client exposes only the plugin-internal size_of
    "graph.from_gdl",  # graphs are constructed from tables or triplets
    "graph.stream",  # the client's session export raises NotImplementedError
    "graph.node_labels.stream",  # node_labels maps mutate and write only
    "graph.relationship_properties.stream",  # served by relationships.stream(relationship_properties=...)
    # The projection protocol: the client drives it from `gds.graph.construct`, so the
    # individual channels it calls are not paths of their own.
    "graph.project.from_tables",
    "graph.project.from_tables.nodes",
    "graph.project.from_tables.relationships",
    "graph.project.from_tables.nodes_done",
    "graph.project.from_tables.relationships_done",
    "graph.project.from_triplets",
    "graph.project.from_triplets.done",
}

# The client's sampling estimate calls the flight server's `v2/graph.sample.cnarw.estimate`,
# which the published spec does not describe: no surface publishes an estimate, so the
# endpoint is not verified against one.

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


def test_session_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec], session_api_spec: ApiSpec) -> None:
    # Algorithms the GDS Sessions API spec describes are validated against it.
    # Everything else the session surface exposes is still validated against the plugin API spec.
    job_spec_endpoints = {pythonic_endpoint_name(endpoint.name) for endpoint in session_api_spec.endpoints()}
    # The plugin's projection procedure: the session surface drives projections through
    # the protocol, whose channels the catalog entries cover.
    job_spec_endpoints.add("graph.project")
    # The deprecated singular procedure: the client's `node_property.stream` is a
    # convenience that runs the plural `nodeProperties` procedure, which the session spec
    # covers.
    job_spec_endpoints.add("graph.node_property.stream")

    assert_api_spec_coverage(
        AuraGraphDataScience,
        gds_api_spec,
        unmapped_endpoints=SESSION_UNMAPPED_ENDPOINTS,
        include_arrow_only=True,
        exclude_endpoints=job_spec_endpoints,
    )
    assert_endpoint_spec_coverage(
        AuraGraphDataScience,
        session_api_spec.endpoints(),
        unmapped_endpoints=SESSION_UNMAPPED_GRAPH_CATALOG_ENDPOINTS,
        client_only_parameters=SESSION_CLIENT_ONLY_PARAMETERS,
    )
