from graphdatascience import GraphDataScience
from tests.integration.procedure_surface.api_spec_coverage_test_helper import (
    UNMAPPED_ENDPOINTS,
    assert_api_spec_coverage,
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


def test_plugin_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    assert_api_spec_coverage(
        GraphDataScience, gds_api_spec, unmapped_endpoints=PLUGIN_UNMAPPED_ENDPOINTS, include_arrow_only=False
    )
