from unittest import mock

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.plugin_v2_endpoints import PluginV2Endpoints
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from tests.integrationV2.procedure_surface.api_spec_coverage_test_helper import (
    assert_api_spec_coverage,
)
from tests.integrationV2.procedure_surface.gds_api_spec import EndpointWithModesSpec


def test_plugin_api_spec_coverage(gds_api_spec: list[EndpointWithModesSpec]) -> None:
    endpoints = PluginV2Endpoints(
        arrow_client=mock.Mock(spec=AuthenticatedArrowClient),
        db_client=mock.Mock(spec=Neo4jQueryRunner),
    )

    assert_api_spec_coverage(endpoints, gds_api_spec)
