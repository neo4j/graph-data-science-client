from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from neo4j import GraphDatabase

from graphdatascience.session.aura_api import (
    AuraApi,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.region_suggester import closest_match
from graphdatascience.session.session_sizes import SessionSizeByMemory


@dataclass
class SessionInfo:
    """
    Represents information about a session.

    Attributes:
        name (str): The name of the session.
        size (str): The size of the session.
    """

    name: str
    size: str

    @classmethod
    def from_specific_instance_details(cls, instance_details: InstanceSpecificDetails) -> SessionInfo:
        return SessionInfo(GdsSessionNameHelper.session_name(instance_details.name), instance_details.memory)


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

    def get_or_create(
        self,
        session_name: str,
        size: SessionSizeByMemory,
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

        connected_instance = self._try_connect(session_name, db_connection)
        # TODO: check instance size and fail if mismatch
        if connected_instance is not None:
            return connected_instance

        db_instance_id = AuraApi.extract_id(db_connection.uri)
        db_instance = self._aura_api.list_instance(db_instance_id)
        if not db_instance:
            raise ValueError(f"Could not find Aura instance with the uri `{db_connection.uri}`")

        region = self._ds_region(db_instance.region, db_instance.cloud_provider)

        create_details = self._aura_api.create_instance(
            GdsSessions._instance_name(session_name), size.value, db_instance.cloud_provider, region
        )
        wait_result = self._aura_api.wait_for_instance_running(create_details.id)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to create session `{session_name}`: {err}")

        gds_user = create_details.username
        gds_url = wait_result.connection_url

        self._change_initial_pw(
            gds_url=gds_url, gds_user=gds_user, initial_pw=create_details.password, new_pw=db_connection.password
        )

        return self._construct_client(session_name=session_name, gds_url=gds_url, db_connection=db_connection)

    def delete(self, session_name: str) -> bool:
        """
        Delete a GDS session.
        Args:
            session_name: the name of the session to delete

        Returns:
            True iff a session was deleted as a result of this call.
        """
        instance_name = GdsSessions._instance_name(session_name)

        candidate_instances = [i for i in self._aura_api.list_instances() if i.name == instance_name]

        if len(candidate_instances) > 1:
            self._fail_ambiguous_session(session_name, candidate_instances)

        if len(candidate_instances) == 1:
            candidate = candidate_instances[0]
            return self._aura_api.delete_instance(candidate.id) is not None

        return False

    def list(self) -> List[SessionInfo]:
        """
        Retrieves the list of GDS sessions visible by the user asscociated by the given api-credentials.

        Returns:
            A list of SessionInfo objects representing the GDS sessions.
        """
        all_instances = self._aura_api.list_instances()
        instance_details = [
            self._aura_api.list_instance(instance_id=instance.id)
            for instance in all_instances
            if GdsSessionNameHelper.is_gds_session(instance)
        ]

        return [
            SessionInfo.from_specific_instance_details(instance_detail)
            for instance_detail in instance_details
            if instance_detail
        ]

    def _try_connect(self, session_name: str, db_connection: DbmsConnectionInfo) -> Optional[AuraGraphDataScience]:
        instance_name = GdsSessions._instance_name(session_name)
        matched_instances = [instance for instance in self._aura_api.list_instances() if instance.name == instance_name]

        if len(matched_instances) == 0:
            return None

        if len(matched_instances) > 1:
            self._fail_ambiguous_session(session_name, matched_instances)

        wait_result = self._aura_api.wait_for_instance_running(matched_instances[0].id)
        if err := wait_result.error:
            raise RuntimeError(f"Failed to connect to session `{session_name}`: {err}")
        gds_url = wait_result.connection_url

        return self._construct_client(session_name=session_name, gds_url=gds_url, db_connection=db_connection)

    def _ds_region(self, region: str, cloud_provider: str) -> str:
        tenant_details = self._aura_api.tenant_details()
        available_regions = tenant_details.regions_per_provider[cloud_provider]

        match = closest_match(region, available_regions)
        if not match:
            raise ValueError(
                f"Tenant `{tenant_details.id}` cannot create GDS sessions at cloud provider `{cloud_provider}`."
            )

        return match

    def _construct_client(
        self, session_name: str, gds_url: str, db_connection: DbmsConnectionInfo
    ) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=DbmsConnectionInfo(
                gds_url, GdsSessions.GDS_SESSION_USER, db_connection.password
            ),
            aura_db_connection_info=db_connection,
            delete_fn=lambda: self.delete(session_name),
        )

    @staticmethod
    def _change_initial_pw(gds_url: str, gds_user: str, initial_pw: str, new_pw: str) -> None:
        with GraphDatabase.driver(gds_url, auth=(gds_user, initial_pw)) as driver:
            driver.execute_query(
                "ALTER CURRENT USER SET PASSWORD FROM $old_pw TO $new_pw",
                parameters_={"old_pw": initial_pw, "new_pw": new_pw},
                database_="system",
            )

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, instances: List[InstanceDetails]) -> None:
        candidates = [(i.id, GdsSessionNameHelper.session_name(i.name)) for i in instances]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )

    @classmethod
    def _instance_name(cls, session_name: str) -> str:
        return GdsSessionNameHelper.instance_name(session_name)


class GdsSessionNameHelper:
    GDS_SESSION_NAME_PREFIX = "gds-session-"

    @classmethod
    def session_name(cls, instance_name: str) -> str:
        # str.removeprefix is only available in Python 3.9+
        return instance_name[len(cls.GDS_SESSION_NAME_PREFIX) :]  # noqa: E203 (black vs flake8 conflict)

    @classmethod
    def instance_name(cls, session_name: str) -> str:
        return f"{cls.GDS_SESSION_NAME_PREFIX}{session_name}"

    @classmethod
    def is_gds_session(cls, instance: InstanceDetails) -> bool:
        return instance.name.startswith(cls.GDS_SESSION_NAME_PREFIX)
