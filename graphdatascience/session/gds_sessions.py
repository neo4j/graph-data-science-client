from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Optional, Union

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.aurads_sessions import AuraDsSessions
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
        tenant (Optional[str]): The tenant for authentication. Needed if a client belongs to multiple tenants.
    """

    client_id: str
    client_secret: str
    tenant: Optional[str] = None


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
        aura_api = AuraApi(api_credentials.client_id, api_credentials.client_secret, api_credentials.tenant)
        session_type_flag = os.environ.get("USE_DEDICATED_SESSIONS", "false").lower() == "true"
        self._impl: Union[DedicatedSessions, AuraDsSessions] = (
            DedicatedSessions(aura_api) if session_type_flag else AuraDsSessions(aura_api)
        )

    def estimate(
        self, node_count: int, relationship_count: int, algorithm_categories: Optional[List[AlgorithmCategory]] = None
    ) -> SessionMemory:
        """
        Estimates the memory required for a session with the given node and relationship counts.

        Args:
            node_count (int): The number of nodes.
            relationship_count (int): The number of relationships.
            algorithm_categories (Optional[List[AlgorithmCategory]]): The algorithm categories to consider.

        Returns:
            SessionMemory: The estimated memory required for the session.
        """
        if algorithm_categories is None:
            algorithm_categories = []
        return self._impl.estimate(node_count, relationship_count, algorithm_categories)

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        """
        Retrieves an existing session with the given session name and database connection,
        or creates a new session if one does not exist.

        Args:
            session_name (str): The name of the session.
            memory (SessionMemory): The size of the session specified by memory.
            db_connection (DbmsConnectionInfo): The database connection information.

        Returns:
            AuraGraphDataScience: The session.
        """
        return self._impl.get_or_create(session_name, memory, db_connection)

    def delete(self, session_name: str) -> bool:
        """
        Delete a GDS session.
        Args:
            session_name: the name of the session to delete

        Returns:
            True iff a session was deleted as a result of this call.
        """
        return self._impl.delete(session_name)

    def list(self) -> List[SessionInfo]:
        """
        Retrieves the list of GDS sessions visible by the user asscociated by the given api-credentials.

        Returns:
            A list of SessionInfo objects representing the GDS sessions.
        """
        return self._impl.list()
