from pytest_mock import MockerFixture

from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.procedure_surface.arrow.catalog.graph_ops_arrow import GraphOpsArrow
from graphdatascience.session.graph_mapping_context import GraphMappingContext
from tests.unit.arrow_client.arrow_test_utils import ArrowTestResult

GRAPH_INFO = {
    "graph_name": "g",
    "database": "db",
    "database_location": "",
    "configuration": {},
    "memory_usage": "0",
    "size_in_bytes": 0,
    "node_count": 0,
    "relationship_count": 0,
    "creation_time": "2020-01-01T00:00:00",
    "modification_time": "2020-01-01T00:00:00",
    "schemaWithOrientation": {},
    "density": 0.0,
}


def test_drop_removes_graph_mappings(mocker: MockerFixture) -> None:
    arrow_client = mocker.Mock(spec=AuthenticatedArrowClient)
    graph_mapping_context = mocker.Mock(spec=GraphMappingContext)
    arrow_client.do_action_with_retry.return_value = [ArrowTestResult(GRAPH_INFO)]

    GraphOpsArrow(arrow_client, graph_mapping_context).drop("g")

    graph_mapping_context.unregister.assert_called_once_with("g")