from __future__ import annotations

import hashlib
import warnings
from datetime import datetime, timedelta, timezone
from typing import List, Optional

import neo4j

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.cloud_location import CloudLocation
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
        ttl: Optional[timedelta] = None,
        cloud_location: Optional[CloudLocation] = None,
    ) -> AuraGraphDataScience:
        self._validate_db_connection(db_connection)

        dbid = AuraApi.extract_id(db_connection.uri)

        # hashing the password to avoid storing the actual db password in Aura
        password = hashlib.sha256(db_connection.password.encode()).hexdigest()

        create_details = self._create_session(session_name, dbid, password, memory.value, ttl, cloud_location)

        if create_details.expiry_date:
            until_expiry: timedelta = create_details.expiry_date - datetime.now(timezone.utc)
            if until_expiry < timedelta(days=1):
                raise Warning(f"Session `{create_details.name}` is expiring in less than a day.")

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

    def delete(self, *, session_name: Optional[str] = None, session_id: Optional[str] = None) -> bool:
        if not session_name and not session_id:
            raise ValueError("Either session_name or session_id must be provided.")

        if session_id:
            return self._aura_api.delete_session(session_id) is not None

        if session_name:
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

        # this will only occur for admins as we cannot resolve Aura-API client_id -> console_user_id we fail for now
        if len(matched_sessions) > 1:
            raise RuntimeError(
                f"The use has access to multiple session with the name `{session_name}`. Please specify the id of the session that should be deleted."
            )

        return matched_sessions[0]

    @staticmethod
    def _validate_db_connection(db_connection):
        with neo4j.GraphDatabase.driver(
            db_connection.uri, auth=(db_connection.username, db_connection.password)
        ) as driver:
            try:
                driver.verify_connectivity()
            except Exception as e:
                raise RuntimeError(f"Failed to connect to the Neo4j database: {e}")
            try:
                driver.verify_authentication()
            except Exception as e:
                raise RuntimeError(f"Failed to authenticate to the Neo4j database: {e}")

    def _create_session(
        self,
        session_name: str,
        dbid: str,
        pwd: str,
        memory: SessionMemoryValue,
        ttl: Optional[timedelta] = None,
        cloud_location: Optional[CloudLocation] = None,
    ) -> SessionDetails:
        db_instance = self._aura_api.list_instance(dbid)

        if not (db_instance or cloud_location):
            raise ValueError("cloud_location must be provided for sessions against a self-managed DB.")

        if cloud_location and db_instance:
            raise ValueError("cloud_location cannot be provided for sessions against an AuraDB.")

        # If cloud location is provided we go for self managed DBs path
        if cloud_location:
            return self._aura_api.create_session(
                name=session_name, pwd=pwd, memory=memory, ttl=ttl, cloud_location=cloud_location
            )
        else:
            return self._aura_api.create_session(name=session_name, dbid=dbid, pwd=pwd, memory=memory, ttl=ttl)

    def _construct_client(
        self, session_id: str, session_connection: DbmsConnectionInfo, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience.create(
            gds_session_connection_info=session_connection,
            db_connection_info=db_connection,
            delete_fn=lambda: self._aura_api.delete_session(session_id=session_id),
        )
