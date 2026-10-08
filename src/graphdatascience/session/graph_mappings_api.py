from __future__ import annotations

from http import HTTPStatus
from typing import Any

from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import GraphMapping


class GraphMappings:
    """
    REST client for the graph-mapping endpoints of the Aura API.

    Graph mappings register which GDS session holds a graph created for a given
    `(database_username, database_uuid)` pair, so that graphs created by one API
    (e.g. this client) can be discovered and used from another (e.g. the Cypher API).
    """

    def __init__(self, aura_api: AuraApi) -> None:
        self._aura_api = aura_api

    def create(
        self,
        session_id: str,
        graph_name: str,
        database_username: str,
        database_uuid: str,
        linked: bool = False,
    ) -> GraphMapping:
        json = {
            "graph_name": graph_name,
            "database_username": database_username,
            "database_uuid": database_uuid,
            "linked": linked,
        }

        response = self._aura_api._request_session.post(
            f"{self._aura_api._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/{session_id}/graphs",
            json=json,
        )

        self._aura_api._check_resp(response)

        raw_json: dict[str, Any] = response.json()
        self._aura_api._check_errors(raw_json)

        return GraphMapping.from_json(raw_json["data"])

    def list(self, database_username: str, database_uuid: str, graph_name: str | None = None) -> list[GraphMapping]:
        params: dict[str, str] = {
            "databaseUsername": database_username,
            "databaseUUID": database_uuid,
        }

        if graph_name is not None:
            params["graphName"] = graph_name

        response = self._aura_api._request_session.get(
            f"{self._aura_api._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/graphs",
            params=params,
        )

        self._aura_api._check_resp(response)

        raw_json: dict[str, Any] = response.json()
        self._aura_api._check_errors(raw_json)

        return [GraphMapping.from_json(m) for m in raw_json["data"]]

    def delete(self, graph_name: str, database_username: str, database_uuid: str) -> bool:
        json = {
            "graph_name": graph_name,
            "database_username": database_username,
            "database_uuid": database_uuid,
        }

        response = self._aura_api._request_session.delete(
            f"{self._aura_api._base_uri}/{AuraApi.API_VERSION}/graph-analytics/sessions/graphs",
            json=json,
        )

        if response.status_code == HTTPStatus.NOT_FOUND.value:
            return False

        self._aura_api._check_resp(response)

        return True
