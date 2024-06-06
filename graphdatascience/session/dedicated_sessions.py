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
from graphdatascience.session.session_sizes import SessionMemory


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

        return SessionMemory(estimation.recommended_size)

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        dbid = AuraApi.extract_id(db_connection.uri)
        existing_session = self._find_existing_session(session_name, dbid)

        # hashing the password to avoid storing the actual db password in Aura
        password = hashlib.sha256(db_connection.password.encode()).hexdigest()

        # TODO configure session size (and check existing_session has same size)
        if existing_session:
            self._check_expiry_date(existing_session)
            session_id = existing_session.id
        else:
            create_details = self._create_session(session_name, dbid, db_connection.uri, password, memory)
            session_id = create_details.id

        wait_result = self._aura_api.wait_for_session_running(session_id, dbid)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to create session `{session_name}`: {err}")

        session_connection = DbmsConnectionInfo(
            uri=wait_result.connection_url,
            username=DedicatedSessions.GDS_SESSION_USER,
            password=password,
        )

        return self._construct_client(
            session_name=session_name, session_connection=session_connection, db_connection=db_connection
        )

    def delete(self, session_name: str, dbid: Optional[str] = None) -> bool:
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

    def list(self) -> List[SessionInfo]:
        dbs = self._aura_api.list_instances()

        sessions: List[SessionDetails] = []
        for db in dbs:
            sessions.extend(self._aura_api.list_sessions(db.id))

        return [SessionInfo.from_session_details(i) for i in sessions]

    def _find_existing_session(self, session_name: str, dbid: str) -> Optional[SessionDetails]:
        matched_sessions = [s for s in self._aura_api.list_sessions(dbid) if s.name == session_name]

        if len(matched_sessions) == 0:
            return None

        if len(matched_sessions) > 1:
            self._fail_ambiguous_session(session_name, matched_sessions)

        return matched_sessions[0]

    def _create_session(
        self, session_name: str, dbid: str, dburi: str, pwd: str, memory: SessionMemory
    ) -> SessionDetails:
        db_instance = self._aura_api.list_instance(dbid)
        if not db_instance:
            raise ValueError(f"Could not find AuraDB instance with the uri `{dburi}`")

        create_details = self._aura_api.create_session(
            name=session_name,
            dbid=dbid,
            pwd=pwd,
            memory=memory.value,
        )
        return create_details

    def _construct_client(
        self, session_name: str, session_connection: DbmsConnectionInfo, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=session_connection,
            aura_db_connection_info=db_connection,
            delete_fn=lambda: self.delete(session_name, dbid=AuraApi.extract_id(db_connection.uri)),
        )

    def _check_expiry_date(self, session: SessionDetails) -> None:
        if session.is_expired():
            raise RuntimeError(f"Session `{session.name}` is expired. Please delete it and create a new one.")
        if session.expiry_date:
            until_expiry: timedelta = session.expiry_date - datetime.now(timezone.utc)
            if until_expiry < timedelta(days=1):
                raise Warning(f"Session `{session.name}` is expiring in less than a day.")

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, sessions: List[SessionDetails]) -> None:
        candidates = [i.id for i in sessions]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )
