from __future__ import annotations

from typing import List, Optional

from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.gds_sessions import AuraAPICredentials


# TODO come up with a good name (explain diff to previous version)
class GdsSessionsV2:
    """
    Class for managing GDS sessions v2 hosted in Neo4j Aura.
    """

    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, api_credentials: AuraAPICredentials) -> None:
        """
        Initializes a new instance of the GdsSessions class.

        Args:
            api_credentials (AuraAPICredentials): The Aura API credentials used for establishing a connection.
        """
        self._aura_api = AuraApi(
            tenant_id=api_credentials.tenant,
            client_id=api_credentials.client_id,
            client_secret=api_credentials.client_secret,
        )

    # TODO configure session size
    def get_or_create(
        self,
        session_name: str,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        """
        Retrieves an existing session with the given session name and database connection,
        or creates a new session if one does not exist.

        Args:
            session_name (str): The name of the session.
            size (SessionSizeByMemory): The size of the session specified by memory.
            db_connection (DbmsConnectionInfo): The database connection information.

        Returns:
            AuraGraphDataScience: The session.
        """

        dbid = AuraApi.extract_id(db_connection.uri)
        existing_session = self._find_existing_session(session_name, dbid)

        if existing_session:
            session_id = existing_session.id
        else:
            create_details = self._create_session(session_name, dbid, db_connection.uri, db_connection.password)
            session_id = create_details.id

        wait_result = self._aura_api.wait_for_session_running(session_id, dbid)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to create session `{session_name}`: {err}")

        return self._construct_client(
            session_name=session_name, gds_url=wait_result.connection_url, db_connection=db_connection
        )

    def delete(self, session_name: str, dbid: Optional[str] = None) -> bool:
        """
        Delete a GDS session.
        Args:
            session_name: the name of the session to delete

        Returns:
            True iff a session was deleted as a result of this call.
        """
        candidate: Optional[SessionDetails] = None
        if not dbid:
            dbs = self._aura_api.list_instances()
            for db in dbs:
                candidate = self._find_existing_session(session_name, db.id)
                if candidate:
                    break
        else:
            candidate = self._find_existing_session(session_name, dbid)

        if candidate:
            return self._aura_api.delete_session(candidate.id, db.id) is not None

        return False

    def list(self) -> List[SessionDetails]:
        """
        Retrieves the list of GDS sessions visible by the user asscociated by the given api-credentials.

        Returns:
            A list of SessionInfo objects representing the GDS sessions.
        """
        dbs = self._aura_api.list_instances()

        sessions: List[SessionDetails] = []
        for db in dbs:
            sessions.extend(self._aura_api.list_sessions(db.id))

        # TODO SessionDetails vs SessionInfo?
        # TODO return dataframe?
        return sessions

    def _find_existing_session(self, session_name: str, dbid: str) -> Optional[SessionDetails]:
        matched_sessions = [s for s in self._aura_api.list_sessions(dbid) if s.name == session_name]

        if len(matched_sessions) == 0:
            return None

        if len(matched_sessions) > 1:
            self._fail_ambiguous_session(session_name, matched_sessions)

        return matched_sessions[0]

    def _create_session(self, session_name: str, dbid: str, dburi: str, pwd: str) -> SessionDetails:
        db_instance = self._aura_api.list_instance(dbid)
        if not db_instance:
            raise ValueError(f"Could not find Aura instance with the uri `{dburi}`")

        create_details = self._aura_api.create_session(
            name=session_name,
            dbid=dbid,
            pwd=pwd,
        )
        return create_details

    def _construct_client(
        self, session_name: str, gds_url: str, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=DbmsConnectionInfo(
                gds_url, GdsSessionsV2.GDS_SESSION_USER, db_connection.password
            ),
            aura_db_connection_info=db_connection,
            delete_fn=lambda: self.delete(session_name, dbid=AuraApi.extract_id(db_connection.uri)),
        )

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, sessions: List[SessionDetails]) -> None:
        candidates = [i.id for i in sessions]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )
