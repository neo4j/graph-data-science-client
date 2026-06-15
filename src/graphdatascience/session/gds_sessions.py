from __future__ import annotations

import math
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import SessionDetails
from graphdatascience.session.aura_api_token_authentication import AuraApiTokenAuthentication
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.dbms.db_environment_resolver import DbEnvironmentResolver
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_lifecycle_manager import SessionLifecycleManager
from graphdatascience.session.session_sizes import SessionMemory, SessionMemoryValue


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

    def create(self, api_credentials: AuraAPICredentials) -> None:
        """
        Create a new instance of the GdsSessions class.

        Parameters
        ----------
        api_credentials
            The Aura API credentials used for establishing a connection.
        """
        aura_env = os.environ.get("AURA_ENV")
        self._aura_api = AuraApi(
            aura_env=aura_env,
            client_id=api_credentials.client_id,
            client_secret=api_credentials.client_secret,
            project_id=api_credentials.project_id,
        )

    def __init__(self, aura_api: AuraApi) -> None:
        """
        Initializes a new instance of the GdsSessions class.

        Parameters
        ----------
        aura_api
            A connector to the Aura API.
        """
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
        """
        Estimates the memory required for a session with the given node and relationship counts.

        Parameters
        ----------
        node_count
            Number of nodes.
        relationship_count
            Number of relationships.
        algorithm_categories
            The algorithm categories to consider.
        node_label_count
            Number of node labels.
        node_property_count
            Number of node properties.
        relationship_property_count
            Number of relationship properties.


        Returns
        -------
        SessionMemory
            The estimated memory required for the session.
        """
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
            Set[CloudLocation]: The list of available cloud locations.
        """
        return list(self._aura_api.project_details().cloud_locations)

    def get_or_create(
        self,
        session_name: str,
        memory: SessionMemory | str,
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
            memory (SessionMemory | SessionMemoryValue | str): The size of the session specified by memory.
            db_connection (DbmsConnectionInfo | None): The database connection information.
            ttl: (timedelta | None): The sessions time to live after inactivity in seconds.
            cloud_location (CloudLocation | None): The cloud location. Required if the GDS session is for a self-managed database.
            timeout (int | None): Optional timeout (in seconds) when waiting for session to become ready. If unset the method will wait forever. If set and session does not become ready an exception will be raised. It is user responsibility to ensure resource gets cleaned up in this situation.
            neo4j_driver_config (dict[str, Any] | None): Optional configuration for the Neo4j driver to the Neo4j DBMS. Only relevant if `db_connection` is specified..
            arrow_client_options (dict[str, Any] | None): Optional configuration for the Arrow Flight client.
        Returns:
            AuraGraphDataScience: The session.
        """
        if isinstance(memory, str) or isinstance(memory, SessionMemoryValue):
            memory = SessionMemory.of(memory)

        if db_connection is None:
            db_runner = None
            aura_db_instance = None
            aura_database_id = None
        else:
            if aura_instance_id := db_connection.aura_instance_id:
                aura_db_instance = self._aura_api.list_instance(aura_instance_id)

                if not aura_db_instance:
                    raise ValueError(
                        f"Aura instance with id `{aura_instance_id}` could not be found. Please verify that the instance id is correct and that you have access to the Aura instance."
                    )

                db_connection.set_uri(aura_db_instance.connection_url)

                db_runner = self._create_db_runner(db_connection, neo4j_driver_config)
            else:
                db_runner = self._create_db_runner(db_connection, neo4j_driver_config)

                if self._check_hosted_in_aura(db_runner):
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
            aura_database_id = db_connection.aura_database_id

        if aura_db_instance is None:
            if not cloud_location:
                raise ValueError("cloud_location must be provided for sessions not attached to an AuraDB.")

            session_details = self._get_or_create_self_managed_session(session_name, memory.value, cloud_location, ttl)
        else:
            if cloud_location is not None:
                raise ValueError("cloud_location cannot be provided for sessions against an AuraDB.")
            session_details = self._get_or_create_attached_session(
                session_name, memory.value, aura_db_instance.id, aura_database_id, ttl
            )

        self._await_session_running(session_details, timeout)

        session_host = session_details.host.split(":")[0]
        session_port = 8491  # TODO we should get this from the API

        arrow_authentication = AuraApiTokenAuthentication(self._aura_api)

        return self._construct_client(
            session_details.id,
            session_host,
            session_port,
            arrow_authentication,
            db_runner,
            arrow_client_options,
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
        if not session_name and not session_id:
            raise ValueError("Either session_name or session_id must be provided.")

        if session_id:
            return self._aura_api.delete_session(session_id) is not None

        if session_name:
            candidate = self._find_existing_session(session_name)
            if candidate:
                return self._aura_api.delete_session(candidate.id) is not None

        return False

    def list(
        self,
        instance_id: str | None = None,
        list_only_owned: bool = False,
        include_deleted: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[SessionInfo]:
        """
        Retrieves the list of GDS sessions visible by the user associated by the given api-credentials.

        Args:
            instance_id: Optional filter for sessions attached to one AuraDB instance.
            list_only_owned: Optional filter to list only sessions owned by the authenticated user.
            include_deleted: Optional flag to include deleted sessions.
            start_date: Optional lower bound for session creation timestamp.
            end_date: Optional upper bound for session creation timestamp.

        Returns:
            A list of SessionInfo objects representing the GDS sessions.
        """
        sessions = self._aura_api.list_sessions(
            instance_id=instance_id,
            list_only_owned=list_only_owned,
            include_deleted=include_deleted,
            start_date=start_date,
            end_date=end_date,
        )
        return [SessionInfo.from_session_details(i) for i in sessions]

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
            if until_expiry < timedelta(hours=1):
                raise Warning(
                    f"Session `{session_details.name}` is expiring in {math.floor(until_expiry.seconds / 60)} minutes."
                )
        if not session_details.is_ready():
            max_wait_time = float(timeout) if timeout is not None else math.inf
            wait_result = self._aura_api.wait_for_session_running(session_details.id, max_wait_time=max_wait_time)
            if err := wait_result.error:
                raise RuntimeError(f"Failed to get or create session `{session_details.name}`: {err}")

    def _find_existing_session(self, session_name: str) -> SessionDetails | None:
        matched_sessions = [s for s in self._aura_api.list_sessions() if s.name == session_name]

        if len(matched_sessions) == 0:
            return None

        # this will only occur for admins as we cannot resolve Aura-API client_id -> console_user_id we fail for now
        if len(matched_sessions) > 1:
            raise RuntimeError(
                f"The user has access to multiple session with the name `{session_name}`. Please specify the id of the session that should be deleted."
            )

        return matched_sessions[0]

    def _check_hosted_in_aura(self, db_runner: Neo4jQueryRunner) -> bool:
        return DbEnvironmentResolver.hosted_in_aura(db_runner)

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
        self,
        session_name: str,
        memory: SessionMemoryValue,
        instance_id: str,
        database_id: str | None = None,
        ttl: timedelta | None = None,
    ) -> SessionDetails:
        return self._aura_api.get_or_create_session(
            name=session_name, instance_id=instance_id, database_id=database_id, memory=memory, ttl=ttl
        )

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
        session_host: str,
        session_port: int,
        arrow_authentication: ArrowAuthentication,
        db_runner: Neo4jQueryRunner | None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience.create(
            (session_host, session_port),
            arrow_authentication,
            db_endpoint=db_runner,
            session_lifecycle_manager=SessionLifecycleManager(session_id, self._aura_api),
            arrow_client_options=arrow_client_options,
        )
