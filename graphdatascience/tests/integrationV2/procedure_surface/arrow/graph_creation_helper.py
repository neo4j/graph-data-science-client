import json
from typing import Optional, Tuple

from graphdatascience import Graph
from graphdatascience.arrow_client.authenticated_flight_client import AuthenticatedArrowClient
from graphdatascience.arrow_client.v2.data_mapper_utils import deserialize_single
from graphdatascience.arrow_client.v2.job_client import JobClient


class MockGraph(Graph):
    def __init__(self, name: str):
        self._name = name

    def name(self) -> str:
        return self._name


def create_graph(
    arrow_client: AuthenticatedArrowClient, graph_name: str, gdl: str, undirected: Optional[Tuple[str, str]] = None
) -> Graph:
    raw_res = arrow_client.do_action(
        "v2/graph.fromGDL", json.dumps({"graphName": graph_name, "gdlGraph": gdl}).encode("utf-8")
    )
    deserialize_single(raw_res)

    if undirected is not None:
        raw_res = JobClient.run_job_and_wait(
            arrow_client,
            "v2/graph.relationships.toUndirected",
            {"graphName": graph_name, "relationshipType": undirected[0], "mutateRelationshipType": undirected[1]},
        )
        deserialize_single(raw_res)

        raw_res = arrow_client.do_action(
            "v2/graph.relationships.drop",
            json.dumps({"graphName": graph_name, "relationshipType": undirected[0]}).encode("utf-8"),
        )
        deserialize_single(raw_res)

    return MockGraph(graph_name)
