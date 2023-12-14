from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Union

from neo4j import GraphDatabase

from graphdatascience.gds_session.aura_api import (
    AuraApi,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.gds_session.aura_graph_data_science import AuraGraphDataScience
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
            GdsSessions.session_name(instance_details.name), instance_details.memory, instance_details.type
        )


@dataclass
class AuraAPICredentials:
    client_id: str
    client_secret: str
    tenant: Optional[str] = None


class GdsSessions:
    GDS_SESSION_NAME_PREFIX = "gds-session-"
    # Hardcoded neo4j user as sessions are always created with this user
    GDS_SESSION_USER = "neo4j"

    def __init__(self, db_connection: DbmsConnectionInfo, ds_connection: AuraAPICredentials) -> None:
        self._db_connection = db_connection
        self._aura_api = AuraApi(
            tenant_id=ds_connection.tenant, client_id=ds_connection.client_id, client_secret=ds_connection.client_secret
        )

    def get_or_create(self, session_name: str, memory: Union[str, SessionSizeByMemory] = "8GB") -> AuraGraphDataScience:
        if session_name in [session.name for session in self.list()]:
            # session exists, connect to it
            return self._connect(session_name, self._db_connection)

        db_instance_id = AuraApi.extract_id(self._db_connection.uri)
        db_instance = self._aura_api.list_instance(db_instance_id)
        if not db_instance:
            raise ValueError(f"Could not find Aura instance with the uri `{self._db_connection.uri}`")

        available_memory_configurations = self._aura_api.list_available_memory_configurations()

        if isinstance(memory, SessionSizeByMemory):
            memory = str(memory.value)

        if memory not in available_memory_configurations:
            raise ValueError(
                (
                    f"Memory configuration `{memory}` is not available. "
                    f"Available configurations are: {available_memory_configurations}"
                )
            )

        create_details = self._aura_api.create_instance(
            GdsSessions._instance_name(session_name), memory, db_instance.cloud_provider, db_instance.region
        )
        wait_result = self._aura_api.wait_for_instance_running(create_details.id)
        if wait_result is not None:
            raise RuntimeError(f"Failed to create session `{session_name}`: {wait_result}")

        gds_user = create_details.username
        gds_url = create_details.connection_url

        self._change_initial_pw(
            gds_url=gds_url, gds_user=gds_user, initial_pw=create_details.password, new_pw=self._db_connection.password
        )

        return self._construct_client(gds_url=gds_url, db_connection=self._db_connection)

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
            if instance.name.startswith(GdsSessions.GDS_SESSION_NAME_PREFIX)
        ]

        return [SessionInfo.from_specific_instance_details(instance_detail) for instance_detail in instance_details]

    def _connect(self, session_name: str, db_connection: DbmsConnectionInfo) -> AuraGraphDataScience:
        instance_name = GdsSessions._instance_name(session_name)
        matched_instances = [instance for instance in self._aura_api.list_instances() if instance.name == instance_name]

        if len(matched_instances) != 1:
            self._fail_ambiguous_session(session_name, matched_instances)

        instance_details = self._aura_api.list_instance(matched_instances[0].id)

        if instance_details:
            gds_url = instance_details.connection_url
        else:
            raise RuntimeError(
                f"Unable to get connection information for session `{session_name}`. Does it still exist?"
            )

        return self._construct_client(gds_url=gds_url, db_connection=db_connection)

    def _change_initial_pw(self, gds_url: str, gds_user: str, initial_pw: str, new_pw: str) -> None:
        with GraphDatabase.driver(gds_url, auth=(gds_user, initial_pw)) as driver:
            driver.execute_query(
                "ALTER CURRENT USER SET PASSWORD FROM $old_pw TO $new_pw",
                parameters_={"old_pw": initial_pw, "new_pw": new_pw},
                database_="system",
            )

    def _construct_client(self, gds_url: str, db_connection: DbmsConnectionInfo) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            gds_session_connection_info=DbmsConnectionInfo(
                gds_url, GdsSessions.GDS_SESSION_USER, db_connection.password
            ),
            aura_db_connection_info=db_connection,
        )

    @classmethod
    def session_name(cls, instance_name: str) -> str:
        # str.removeprefix is only available in Python 3.9+
        return instance_name[len(cls.GDS_SESSION_NAME_PREFIX) :]  # noqa: E203 (black vs flake8 conflict)

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, instances: List[InstanceDetails]) -> None:
        candidates = [(i.id, cls.session_name(i.name)) for i in instances]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )

    @classmethod
    def _instance_name(cls, session_name: str) -> str:
        return f"{cls.GDS_SESSION_NAME_PREFIX}{session_name}"
