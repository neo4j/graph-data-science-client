import pytest

from graphdatascience import QueryRunner, ServerVersion
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.session_v2_endpoints import SessionV2Endpoints
from graphdatascience.tests.integrationV2.procedure_surface.arrow.graph_creation_helper import TestArrowGraph


@pytest.fixture(scope="package")
def gds(arrow_client: AuthenticatedArrowClient, db_query_runner: QueryRunner) -> AuraGraphDataScience:
    return AuraGraphDataScience(
        query_runner=db_query_runner,
        delete_fn=lambda: True,
        gds_version=ServerVersion.from_string("1.2.3"),
        v2_endpoints=SessionV2Endpoints(arrow_client, db_query_runner),
    )


@pytest.mark.db_integration
def test_walking_skeleton(gds: AuraGraphDataScience) -> None:
    project_result = gds.v2.graph.project("g", "RETURN gds.graph.project.remote(0, 1)")
    G = TestArrowGraph(project_result.graph_name)

    wcc_mutate_result = gds.v2.wcc.mutate(G, mutate_property="wcc")

    assert wcc_mutate_result.component_count == 1
