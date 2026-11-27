from __future__ import annotations

import dataclasses

from graphdatascience import GraphDataScience
from tests.integration.procedure_surface.api_spec import ApiSpec, EndpointSpec
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
    graph_catalog_endpoints,
    pythonic_endpoint_name,
)
from tests.integration.procedure_surface.endpoint_spec_coverage_test_helper import (
    assert_endpoint_spec_coverage,
)
from tests.integration.procedure_surface.gds_api_spec import EndpointWithModesSpec

# endpoints that are only available in AGA (GDS Sessions), not in the self-managed GDS plugin
PLUGIN_UNMAPPED_ENDPOINTS = UNMAPPED_ENDPOINTS | {
    "fast_path.mutate",  # python-runtime backed, AGA only
    "fast_path.stream",  # python-runtime backed, AGA only
    "fast_path.write",  # python-runtime backed, AGA only
    "graph_sage.supervised.train",  # python-runtime backed, AGA only
    "graph_sage.supervised.stream",  # python-runtime backed, AGA only
    "graph_sage.supervised.write",  # python-runtime backed, AGA only
    "graph_sage.supervised.mutate",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.train",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.stream",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.write",  # python-runtime backed, AGA only
    "graph_sage.unsupervised.mutate",  # python-runtime backed, AGA only
}

# Graph catalog endpoints the plugin surface does not map onto their own path. Drop
# entries one at a time as the client grows them.
PLUGIN_UNMAPPED_GRAPH_CATALOG_ENDPOINTS = {
    "graph.relationship.write",  # served by the client's relationships endpoints
    # The deprecated singular procedure; the client's own `node_property.stream` is a
    # convenience that runs the plural `nodeProperties` procedure.
    "graph.node_property.stream",
    # The deprecated legacy projection: the client's own `project.cypher` runs the modern
    # Cypher projection, which the server publishes as an aggregation function.
    "graph.project.cypher",
}


def _plugin_endpoints(spec: ApiSpec) -> list[EndpointSpec]:
    # the plugin spec publishes procedure names (`gds.articleRank`); the client path drops the procedure prefix
    return [
        dataclasses.replace(endpoint, endpoint_name=endpoint.endpoint_name.removeprefix("gds."))
        for endpoint in spec.endpoints()
    ]


def test_plugin_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec], plugin_api_spec: ApiSpec) -> None:
    plugin_endpoints = _plugin_endpoints(plugin_api_spec)
    plugin_spec_endpoints = {pythonic_endpoint_name(endpoint.name) for endpoint in plugin_endpoints}

    # Algorithms the plugin API spec describes are validated against it. The graph
    # catalog is validated against the published specs alone, never against the
    # reflection document. Everything else the plugin surface exposes (model, pipeline
    # and utility procedures) is validated against the legacy plugin API spec.
    assert_api_spec_coverage(
        GraphDataScience,
        gds_api_spec,
        unmapped_endpoints=PLUGIN_UNMAPPED_ENDPOINTS,
        exclude_endpoints=plugin_spec_endpoints | graph_catalog_endpoints(gds_api_spec),
    )
    assert_endpoint_spec_coverage(
        GraphDataScience,
        plugin_endpoints,
        unmapped_endpoints=PLUGIN_UNMAPPED_GRAPH_CATALOG_ENDPOINTS,
    )
