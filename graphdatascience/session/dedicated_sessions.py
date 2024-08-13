from __future__ import annotations

import hashlib
import warnings
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory, SessionMemoryValue


class DedicatedSessions:
    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, aura_api: AuraApi) -> None:
        self._aura_api = aura_api

    def estimate(
        self, node_count: int, relationship_count: int, algorithm_categories: Optional[List[AlgorithmCategory]] = None
    ) -> SessionMemory:
        if algorithm_categories is None:
            algorithm_categories = []
        estimation = self._aura_api.estimate_size(node_count, relationship_count, algorithm_categories)

        if estimation.did_exceed_maximum:
            warnings.warn(
                f"The estimated memory `{estimation.min_required_memory}` exceeds the maximum size"
                f" supported by your Aura tenant (`{estimation.recommended_size}`).",
                ResourceWarning,
            )

        return SessionMemory(SessionMemoryValue(estimation.recommended_size))

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        dbid = AuraApi.extract_id(db_connection.uri)
        existing_session = self._find_existing_session(session_name)

        # hashing the password to avoid storing the actual db password in Aura
        password = hashlib.sha256(db_connection.password.encode()).hexdigest()

        if existing_session:
            self._check_expiry_date(existing_session)
            self._check_memory_configuration(existing_session, memory.value)
            session_id = existing_session.id
        else:
            create_details = self._create_session(session_name, dbid, db_connection.uri, password, memory.value)
            session_id = create_details.id

        wait_result = self._aura_api.wait_for_session_running(session_id)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to create session `{session_name}`: {err}")

        session_connection = DbmsConnectionInfo(
            uri=wait_result.connection_url,
            username=DedicatedSessions.GDS_SESSION_USER,
            password=password,
        )

        return self._construct_client(
            session_id=session_id, session_connection=session_connection, db_connection=db_connection
        )

    def delete(self, session_name: str) -> bool:
        candidate = self._find_existing_session(session_name)

        if candidate:
            return self._aura_api.delete_session(candidate.id) is not None

        return False

    def list(self, dbid: Optional[str] = None) -> List[SessionInfo]:
        sessions: List[SessionDetails] = self._aura_api.list_sessions(dbid)

        return [SessionInfo.from_session_details(i) for i in sessions]

    def _find_existing_session(self, session_name: str) -> Optional[SessionDetails]:
        matched_sessions: List[SessionDetails] = []
        matched_sessions = [s for s in self._aura_api.list_sessions() if s.name == session_name]

        if len(matched_sessions) == 0:
            return None

        return matched_sessions[0]

    def _create_session(
        self, session_name: str, dbid: str, dburi: str, pwd: str, memory: SessionMemoryValue
    ) -> SessionDetails:
        db_instance = self._aura_api.list_instance(dbid)
        if not db_instance:
            raise ValueError(f"Could not find AuraDB instance with the uri `{dburi}`")

        create_details = self._aura_api.create_session(
            name=session_name,
            dbid=dbid,
            pwd=pwd,
            memory=memory,
        )
        return create_details

    def _construct_client(
        self, session_id: str, session_connection: DbmsConnectionInfo, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=session_connection,
            aura_db_connection_info=db_connection,
            delete_fn=lambda: self._aura_api.delete_session(session_id=session_id),
        )

    def _check_expiry_date(self, session: SessionDetails) -> None:
        if session.is_expired():
            raise RuntimeError(f"Session `{session.name}` is expired. Please delete it and create a new one.")
        if session.expiry_date:
            until_expiry: timedelta = session.expiry_date - datetime.now(timezone.utc)
            if until_expiry < timedelta(days=1):
                raise Warning(f"Session `{session.name}` is expiring in less than a day.")

    def _check_memory_configuration(
        self, existing_session: SessionDetails, requested_memory: SessionMemoryValue
    ) -> None:
        if existing_session.memory != requested_memory:
            raise RuntimeError(
                f"Session `{existing_session.name}` exists with a different memory configuration. "
                f"Current: {existing_session.memory}, Requested: {requested_memory}."
            )
