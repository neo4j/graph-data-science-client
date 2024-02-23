from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

from neo4j import GraphDatabase

from graphdatascience.gds_session.aura_api import (
    AuraApi,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.gds_session.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.gds_session.cloud_location_resolver import CloudLocationResolver
from graphdatascience.gds_session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.gds_session.session_sizes import SessionSizeByMemory


@dataclass
class SessionInfo:
    name: str
    size: str
    type: str

    @classmethod
    def from_specific_instance_details(cls, instance_details: InstanceSpecificDetails) -> SessionInfo:
        return SessionInfo(
            GdsSessionNameHelper.session_name(instance_details.name), instance_details.memory, instance_details.type
        )


@dataclass
class AuraAPICredentials:
    client_id: str
    client_secret: str
    tenant: Optional[str] = None


class GdsSessions:
    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, ds_connection: AuraAPICredentials) -> None:
        self._aura_api = AuraApi(
            tenant_id=ds_connection.tenant, client_id=ds_connection.client_id, client_secret=ds_connection.client_secret
        )
        self.__location_resolver: Optional[CloudLocationResolver] = None

    def set_cloud_location(self, cloud_provider: str, region: str) -> None:
        self._location_resolver().set_default(cloud_provider, region)

    def get_or_create(
        self,
        session_name: str,
        size: SessionSizeByMemory,
        db_connection: DbmsConnectionInfo,
    ) -> AuraGraphDataScience:
        connected_instance = self._try_connect(session_name, db_connection)
        if connected_instance is not None:
            return connected_instance

        db_instance_id = AuraApi.extract_id(db_connection.uri)
        aura_db_instance = self._aura_api.list_instance(db_instance_id)

        if not aura_db_instance:
            location = self._location_resolver().default_location()
            if not location:
                raise ValueError(" Please set a cloud_location to create sessions for self-hosted dbs.")
        else:
            location = self._location_resolver().for_instance(aura_db_instance)

        create_details = self._aura_api.create_instance(
            GdsSessions._instance_name(session_name), size.value, location.cloud_provider, location.region
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

    def _location_resolver(self) -> CloudLocationResolver:
        # initialize lazy to avoid calls on __init__
        if not self.__location_resolver:
            self.__location_resolver = CloudLocationResolver(self._aura_api.tenant_details())
        return self.__location_resolver

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
