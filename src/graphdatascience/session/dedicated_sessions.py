from __future__ import annotations

import math
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any

from graphdatascience.query_runner.arrow_authentication import ArrowAuthentication
from graphdatascience.query_runner.db_environment_resolver import DbEnvironmentResolver
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.aura_api_token_authentication import AuraApiTokenAuthentication
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_sizes import SessionMemory, SessionMemoryValue


class DedicatedSessions:
    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, aura_api: AuraApi) -> None:
        self._aura_api = aura_api

    def estimate(
        self,
        node_count: int,
        relationship_count: int,
        algorithm_categories: list[AlgorithmCategory] | list[str] | None = None,
        node_label_count: int = 0,
        node_property_count: int = 0,
        relationship_property_count: int = 0,
    ) -> SessionMemory:
        if algorithm_categories is None:
            algorithm_categories = []
        else:
            algorithm_categories = [
                AlgorithmCategory(cat) if isinstance(cat, str) else cat for cat in algorithm_categories
            ]
        estimation = self._aura_api.estimate_size(
            node_count=node_count,
            node_label_count=node_label_count,
            node_property_count=node_property_count,
            relationship_count=relationship_count,
            relationship_property_count=relationship_property_count,
            algorithm_categories=algorithm_categories,
        )

        if estimation.exceeds_recommended():
            warnings.warn(
                f"The estimated memory `{estimation.estimated_memory}` exceeds the maximum size"
                f" supported by your Aura project (`{estimation.recommended_size}`).",
                ResourceWarning,
            )

        return SessionMemory(SessionMemoryValue(estimation.recommended_size))

    def available_cloud_locations(self) -> list[CloudLocation]:
        """
        Retrieves the list of available cloud locations in Aura.

        Returns:
            list[CloudLocation]: The list of available cloud locations.
        """
        # return a list to allow index based access
        return list(self._aura_api.project_details().cloud_locations)

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory | SessionMemoryValue | str,
        db_connection: DbmsConnectionInfo | None = None,
        ttl: timedelta | None = None,
        cloud_location: CloudLocation | None = None,
        timeout: int | None = None,
        neo4j_driver_options: dict[str, Any] | None = None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> AuraGraphDataScience:
        if isinstance(memory, str) | isinstance(memory, SessionMemoryValue):
            memory = SessionMemory.of(memory)

        if db_connection is None:
            db_runner = None
            aura_db_instance = None
        else:
            if aura_instance_id := db_connection.aura_instance_id:
                aura_db_instance = self._aura_api.list_instance(aura_instance_id)

                if not aura_db_instance:
                    raise ValueError(
                        f"Aura instance with id `{aura_instance_id}` could not be found. Please verify that the instance id is correct and that you have access to the Aura instance."
                    )

                db_connection.set_uri(aura_db_instance.connection_url)

                db_runner = self._create_db_runner(db_connection, neo4j_driver_options)
            else:
                db_runner = self._create_db_runner(db_connection, neo4j_driver_options)

                if DbEnvironmentResolver.hosted_in_aura(db_runner):
                    warnings.warn(
                        DeprecationWarning(
                            "Deriving the Aura instance from the database URI is deprecated and will be removed in a future release. "
                            "Please specify the `aura_instance_id` in the `db_connection` argument."
                        )
                    )

                    aura_instance_id = AuraApi.extract_id(db_connection.get_uri())
                    aura_db_instance = self._aura_api.list_instance(aura_instance_id)
                    if not aura_db_instance:
                        raise ValueError(
                            f"Aura instance with id `{aura_instance_id}` could not be found. Please specify the `aura_instance_id` in the `db_connection` argument."
                        )
                else:
                    aura_db_instance = None

        if aura_db_instance is None:
            if not cloud_location:
                raise ValueError("cloud_location must be provided for sessions not attached to an AuraDB.")

            session_details = self._get_or_create_self_managed_session(session_name, memory.value, cloud_location, ttl)
        else:
            if cloud_location is not None:
                raise ValueError("cloud_location cannot be provided for sessions against an AuraDB.")
            session_details = self._get_or_create_attached_session(session_name, memory.value, aura_db_instance.id, ttl)

        self._await_session_running(session_details, timeout)

        session_bolt_connection_info = DbmsConnectionInfo(
            uri=session_details.bolt_connection_url(),
            username=self._aura_api._credentials[0],
            password=self._aura_api._credentials[1],
        )

        arrow_authentication = AuraApiTokenAuthentication(self._aura_api)

        return self._construct_client(
            session_id=session_details.id,
            session_bolt_connection_info=session_bolt_connection_info,
            arrow_authentication=arrow_authentication,
            db_runner=db_runner,
            arrow_client_options=arrow_client_options,
        )

    def _create_db_runner(
        self, db_connection: DbmsConnectionInfo, config: dict[str, Any] | None = None
    ) -> Neo4jQueryRunner:
        db_runner = Neo4jQueryRunner.create_for_db(
            endpoint=db_connection.get_uri(),
            auth=db_connection.get_auth(),
            aura_ds=True,
            show_progress=False,
            database=db_connection.database,
            config=config,
        )
        self._validate_db_connection(db_runner)
        return db_runner

    def _await_session_running(self, session_details: SessionDetails, timeout: int | None = None) -> None:
        if session_details.expiry_date:
            until_expiry: timedelta = session_details.expiry_date - datetime.now(timezone.utc)
            if until_expiry < timedelta(days=1):
                raise Warning(f"Session `{session_details.name}` is expiring in less than a day.")
        if not session_details.is_ready():
            max_wait_time = float(timeout) if timeout is not None else math.inf
            wait_result = self._aura_api.wait_for_session_running(session_details.id, max_wait_time=max_wait_time)
            if err := wait_result.error:
                raise RuntimeError(f"Failed to get or create session `{session_details.name}`: {err}")

    def delete(self, *, session_name: str | None = None, session_id: str | None = None) -> bool:
        if not session_name and not session_id:
            raise ValueError("Either session_name or session_id must be provided.")

        if session_id:
            return self._aura_api.delete_session(session_id) is not None

        if session_name:
            candidate = self._find_existing_session(session_name)
            if candidate:
                return self._aura_api.delete_session(candidate.id) is not None

        return False

    def list(self, dbid: str | None = None) -> list[SessionInfo]:
        sessions = self._aura_api.list_sessions(dbid)
        return [SessionInfo.from_session_details(i) for i in sessions]

    def _find_existing_session(self, session_name: str) -> SessionDetails | None:
        matched_sessions: list[SessionDetails] = []
        matched_sessions = [s for s in self._aura_api.list_sessions() if s.name == session_name]

        if len(matched_sessions) == 0:
            return None

        # this will only occur for admins as we cannot resolve Aura-API client_id -> console_user_id we fail for now
        if len(matched_sessions) > 1:
            raise RuntimeError(
                f"The user has access to multiple session with the name `{session_name}`. Please specify the id of the session that should be deleted."
            )

        return matched_sessions[0]

    @staticmethod
    def _validate_db_connection(db_runner: Neo4jQueryRunner) -> None:
        try:
            db_runner.verify_connectivity()
        except Exception as e:
            raise RuntimeError(f"Failed to connect to the Neo4j database: {e}")
        try:
            db_runner.verify_authentication()
        except Exception as e:
            raise RuntimeError(f"Failed to authenticate to the Neo4j database: {e}")

    def _get_or_create_standalone_session(
        self,
        session_name: str,
        memory: SessionMemoryValue,
        cloud_location: CloudLocation,
        ttl: timedelta | None = None,
    ) -> SessionDetails:
        return self._aura_api.get_or_create_session(session_name, memory, ttl=ttl, cloud_location=cloud_location)

    def _get_or_create_attached_session(
        self, session_name: str, memory: SessionMemoryValue, instance_id: str, ttl: timedelta | None = None
    ) -> SessionDetails:
        return self._aura_api.get_or_create_session(name=session_name, instance_id=instance_id, memory=memory, ttl=ttl)

    def _get_or_create_self_managed_session(
        self,
        session_name: str,
        memory: SessionMemoryValue,
        cloud_location: CloudLocation,
        ttl: timedelta | None = None,
    ) -> SessionDetails:
        return self._aura_api.get_or_create_session(
            name=session_name,
            memory=memory,
            ttl=ttl,
            cloud_location=cloud_location,
        )

    def _construct_client(
        self,
        session_id: str,
        session_bolt_connection_info: DbmsConnectionInfo,
        arrow_authentication: ArrowAuthentication,
        db_runner: Neo4jQueryRunner | None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience.create(
            session_bolt_connection_info=session_bolt_connection_info,
            arrow_authentication=arrow_authentication,
            db_endpoint=db_runner,
            session_lifecycle_manager=SessionLifecycleManager(session_id, self._aura_api),
            arrow_client_options=arrow_client_options,
        )
