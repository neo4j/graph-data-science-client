from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple, Type

from neo4j import GraphDatabase

from graphdatascience import GraphDataScience
from graphdatascience.aura_api import AuraApi
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

    # TODO wrapper around it for closable?
    def create_gds(self, session_name: str, session_password: str) -> GraphDataScience:
        if len(session_password) < 8:
            raise ValueError("Password must be at least 8 characters long.")

        if session_name in [session.name for session in self.list_sessions()]:
            raise RuntimeError(f"Session with name `{session_name}` already exists.")

        create_details = self._aura_api.create_instance(AuraSessions._instance_name(session_name))
        self._aura_api.wait_for_instance_running(create_details.id)

        gds_user = create_details.username
        url = create_details.connection_url

        self._change_initial_pw(
            gds_url=url, gds_user=gds_user, initial_pw=create_details.password, new_pw=session_password
        )

        return self._construct_client(gds_url=url, gds_user=gds_user, gds_pw=session_password)

    def connect(self, session_name: str, session_password: str) -> GraphDataScience:
        instance_name = AuraSessions._instance_name(session_name)
        matched_instances = [
            instance.id for instance in self._aura_api.list_instances() if instance.name == instance_name
        ]

        if len(matched_instances) != 1:
            raise ValueError("TODO")

        instance_details = self._aura_api.list_instance(matched_instances[0])

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
            candidate_names = [
                (i.id, i.name.removeprefix(AuraSessions.GDS_SESSION_NAME_PREFIX)) for i in candidate_instances
            ]
            session_name = instance_name.removeprefix(AuraSessions.GDS_SESSION_NAME_PREFIX)
            raise RuntimeError(
                f"Expected to find exactly one instance with name `{session_name}`, but found `{candidate_names}`"
            )

        if len(candidate_instances) == 1:
            candidate = candidate_instances[0]
            return self._aura_api.delete_instance(candidate.id) is not None

        return False

    def list_sessions(self) -> List[SessionInfo]:
        all_instances = self._aura_api.list_instances()

        return [
            SessionInfo(instance.name.removeprefix(AuraSessions.GDS_SESSION_NAME_PREFIX))
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

    def _construct_client(self, gds_url: str, gds_user: str, gds_pw: str) -> GraphDataScience:
        return GraphDataScience(
            endpoint=gds_url, auth=(gds_user, gds_pw), aura_ds=True, aura_db_connection_info=self._db_credentials
        )

    @classmethod
    def _instance_name(cls: Type[AuraSessions], session_name: str) -> str:
        return f"{AuraSessions.GDS_SESSION_NAME_PREFIX}{session_name}"
