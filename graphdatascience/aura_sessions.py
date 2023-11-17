from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

from neo4j import GraphDatabase

from graphdatascience.aura_api import AuraApi, InstanceDetails
from graphdatascience.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)


@dataclass
class SessionInfo:
    name: str


class AuraSessions:
    GDS_SESSION_NAME_PREFIX = "gds-session-"

    def __init__(
        self,
        db_credentials: AuraDbConnectionInfo,
        aura_api_client_auth: Tuple[str, str],
        tenant_id: Optional[str] = None,
    ) -> None:
        self._db_credentials = db_credentials
        self._aura_api = AuraApi(
            tenant_id=tenant_id, client_id=aura_api_client_auth[0], client_secret=aura_api_client_auth[1]
        )

    def create_gds(self, session_name: str, session_password: str, memory: str = "8GB") -> AuraGraphDataScience:
        if len(session_password) < 8:
            raise ValueError("Password must be at least 8 characters long.")

        if session_name in [session.name for session in self.list_sessions()]:
            raise RuntimeError(f"Session with name `{session_name}` already exists.")

        db_instance_id = AuraApi.extract_id(self._db_credentials.uri)
        db_instance = self._aura_api.list_instance(db_instance_id)
        if not db_instance:
            raise ValueError(f"Could not find Aura instance with the uri `{self._db_credentials.uri}`")

        available_memory_configurations = self._aura_api.list_available_memory_configurations()
        if memory not in available_memory_configurations:
            raise ValueError(
                (
                    f"Memory configuration `{memory}` is not available. "
                    f"Available configurations are: {available_memory_configurations}"
                )
            )

        create_details = self._aura_api.create_instance(
            AuraSessions._instance_name(session_name), memory, db_instance.cloud_provider, db_instance.region
        )
        wait_result = self._aura_api.wait_for_instance_running(create_details.id)
        if wait_result is not None:
            raise RuntimeError(f"Failed to create session `{session_name}`: {wait_result}")

        gds_user = create_details.username
        gds_url = create_details.connection_url

        self._change_initial_pw(
            gds_url=gds_url, gds_user=gds_user, initial_pw=create_details.password, new_pw=session_password
        )

        return self._construct_client(gds_url=gds_url, gds_user=gds_user, gds_pw=session_password)

    def connect(self, session_name: str, session_password: str) -> AuraGraphDataScience:
        instance_name = AuraSessions._instance_name(session_name)
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

        # Hardcoded neo4j user as sessions are always created with this user
        return self._construct_client(gds_url=gds_url, gds_user="neo4j", gds_pw=session_password)

    def delete_gds(self, session_name: str) -> bool:
        """
        Delete a GDS session.
        Args:
            session_name: the name of the session to delete

        Returns:
            True iff a session was deleted as a result of this call.
        """
        instance_name = AuraSessions._instance_name(session_name)

        candidate_instances = [i for i in self._aura_api.list_instances() if i.name == instance_name]

        if len(candidate_instances) > 1:
            self._fail_ambiguous_session(session_name, candidate_instances)

        if len(candidate_instances) == 1:
            candidate = candidate_instances[0]
            return self._aura_api.delete_instance(candidate.id) is not None

        return False

    def list_sessions(self) -> List[SessionInfo]:
        all_instances = self._aura_api.list_instances()

        return [
            SessionInfo(AuraSessions._session_name(instance))
            for instance in all_instances
            if instance.name.startswith(AuraSessions.GDS_SESSION_NAME_PREFIX)
        ]

    def _change_initial_pw(self, gds_url: str, gds_user: str, initial_pw: str, new_pw: str) -> None:
        with GraphDatabase.driver(gds_url, auth=(gds_user, initial_pw)) as driver:
            driver.execute_query(
                "ALTER CURRENT USER SET PASSWORD FROM $old_pw TO $new_pw",
                parameters_={"old_pw": initial_pw, "new_pw": new_pw},
                database_="system",
            )

    def _construct_client(self, gds_url: str, gds_user: str, gds_pw: str) -> AuraGraphDataScience:
        return AuraGraphDataScience(
            endpoint=gds_url, auth=(gds_user, gds_pw), aura_db_connection_info=self._db_credentials
        )

    @classmethod
    def _fail_ambiguous_session(cls, session_name: str, instances: List[InstanceDetails]) -> None:
        candidates = [(i.id, cls._session_name(i)) for i in instances]
        raise RuntimeError(
            f"Expected to find exactly one GDS session with name `{session_name}`, but found `{candidates}`."
        )

    @classmethod
    def _instance_name(cls, session_name: str) -> str:
        return f"{cls.GDS_SESSION_NAME_PREFIX}{session_name}"

    @classmethod
    def _session_name(cls, instance: InstanceDetails) -> str:
        # str.removeprefix is only available in Python 3.9+
        return instance.name[len(cls.GDS_SESSION_NAME_PREFIX) :]  # noqa: E203 (black vs flake8 conflict)
