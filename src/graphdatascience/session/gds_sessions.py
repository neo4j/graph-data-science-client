from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.dedicated_sessions import DedicatedSessions
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory


@dataclass
class AuraAPICredentials:
    """
    Represents the credentials required for accessing the Aura API.

    Attributes:
        client_id (str): The client ID for authentication.
        client_secret (str): The client secret for authentication.
        project_id (str | None): The project ID for authentication. Needed if a client belongs to multiple projects.
    """

    client_id: str
    client_secret: str
    project_id: str | None = None

    @staticmethod
    def from_env() -> AuraAPICredentials:
        """
        Create an AuraApi instance from environment variables.
        The environment variables are:
        - CLIENT_ID
        - CLIENT_SECRET
        - PROJECT_ID
        """
        client_id = os.environ["CLIENT_ID"]
        client_secret = os.environ["CLIENT_SECRET"]
        project_id = os.environ.get("PROJECT_ID")

        return AuraAPICredentials(client_id, client_secret, project_id)


class GdsSessions:
    """
    Primary API class for managing GDS sessions hosted in Neo4j Aura.
    """

    def __init__(self, api_credentials: AuraAPICredentials) -> None:
        """
        Initializes a new instance of the GdsSessions class.

        Args:
            api_credentials (AuraAPICredentials): The Aura API credentials used for establishing a connection.
        """
        aura_env = os.environ.get("AURA_ENV")
        aura_api = AuraApi(
            aura_env=aura_env,
            client_id=api_credentials.client_id,
            client_secret=api_credentials.client_secret,
            project_id=api_credentials.project_id,
        )
        self._impl: DedicatedSessions = DedicatedSessions(aura_api)

    def estimate(
        self,
        node_count: int,
        relationship_count: int,
        algorithm_categories: Optional[list[AlgorithmCategory]] = None,
        node_label_count: int = 0,
        node_property_count: int = 0,
        relationship_property_count: int = 0,
    ) -> SessionMemory:
        """
        Estimates the memory required for a session with the given node and relationship counts.

        Args:
            node_count (int): The number of nodes.
            relationship_count (int): The number of relationships.
            algorithm_categories (Optional[list[AlgorithmCategory]]): The algorithm categories to consider.
            node_label_count (int): The number of node labels.
            node_property_count (int): The number of node properties.
            relationship_property_count (int): The number of relationship properties.
        Returns:
            SessionMemory: The estimated memory required for the session.
        """
        if algorithm_categories is None:
            algorithm_categories = []
        return self._impl.estimate(
            node_count=node_count,
            relationship_count=relationship_count,
            algorithm_categories=algorithm_categories,
            node_label_count=node_label_count,
            node_property_count=node_property_count,
            relationship_property_count=relationship_property_count,
        )

    def available_cloud_locations(self) -> list[CloudLocation]:
        """
        Retrieves the list of available cloud locations in Aura.

        Returns:
            Set[CloudLocation]: The list of available cloud locations.
        """
        return self._impl.available_cloud_locations()

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory,
        db_connection: DbmsConnectionInfo | None = None,
        ttl: timedelta | None = None,
        cloud_location: CloudLocation | None = None,
        timeout: int | None = None,
        neo4j_driver_config: dict[str, Any] | None = None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> AuraGraphDataScience:
        """
        Retrieves an existing session with the given session name and database connection,
        or creates a new session if one does not exist.

        If the session is close to expiration, a warning will be raised.
        If the session failed, an exception will be raised.

        Args:
            session_name (str): The name of the session.
            memory (SessionMemory): The size of the session specified by memory.
            db_connection (DbmsConnectionInfo | None): The database connection information.
            ttl: (timedelta | None): The sessions time to live after inactivity in seconds.
            cloud_location (CloudLocation | None): The cloud location. Required if the GDS session is for a self-managed database.
            timeout (int | None): Optional timeout (in seconds) when waiting for session to become ready. If unset the method will wait forever. If set and session does not become ready an exception will be raised. It is user responsibility to ensure resource gets cleaned up in this situation.
            neo4j_driver_config (dict[str, Any] | None): Optional configuration for the Neo4j driver to the Neo4j DBMS. Only relevant if `db_connection` is specified..
            arrow_client_options (dict[str, Any] | None): Optional configuration for the Arrow Flight client.
        Returns:
            AuraGraphDataScience: The session.
        """
        return self._impl.get_or_create(
            session_name,
            memory,
            db_connection=db_connection,
            ttl=ttl,
            cloud_location=cloud_location,
            timeout=timeout,
            neo4j_driver_options=neo4j_driver_config,
            arrow_client_options=arrow_client_options,
        )

    def delete(self, *, session_name: str | None = None, session_id: str | None = None) -> bool:
        """
        Delete a GDS session either by name or id.
        Args:
            session_name: the name of the session to delete
            session_id: the id of the session to delete

        Returns:
            True iff a session was deleted as a result of this call.
        """
        return self._impl.delete(session_name=session_name, session_id=session_id)

    def list(self) -> list[SessionInfo]:
        """
        Retrieves the list of GDS sessions visible by the user associated by the given api-credentials.

        Returns:
            A list of SessionInfo objects representing the GDS sessions.
        """
        return self._impl.list()
