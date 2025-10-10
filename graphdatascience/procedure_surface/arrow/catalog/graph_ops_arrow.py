from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize
from graphdatascience.procedure_surface.api.catalog.graph_info import GraphInfo, GraphInfoWithDegrees
from graphdatascience.procedure_surface.utils.config_converter import ConfigConverter


class GraphOpsArrow:
    def __init__(self, arrow_client: AuthenticatedArrowClient):
        self._arrow_client = arrow_client

    def list(self, graph_name: str | None = None) -> list[GraphInfoWithDegrees]:
        payload = {"graphName": graph_name} if graph_name else {}

        result = self._arrow_client.do_action_with_retry("v2/graph.list", payload)

        return [GraphInfoWithDegrees(**row) for row in deserialize(result)]

    def drop(self, graph_name: str, fail_if_missing: bool | None = None) -> GraphInfo | None:
        config = ConfigConverter.convert_to_gds_config(graph_name=graph_name, fail_if_missing=fail_if_missing)
        result = self._arrow_client.do_action_with_retry("v2/graph.drop", config)
        deserialized_results = deserialize(result)

        if len(deserialized_results) == 1:
            return GraphInfo(**deserialized_results[0])
        else:
            return None
