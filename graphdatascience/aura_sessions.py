from __future__ import annotations
from dataclasses import dataclass
from typing import List, Tuple, Type

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

    def __init__(self, db_credentials: AuraDbConnectionInfo, aura_api_client_auth: Tuple[str, str]) -> None:
        self._db_credentials = db_credentials
        self._aura_api = AuraApi(aura_api_client_auth[0], aura_api_client_auth[1])

    # TODO wrapper around it for closable?
    # TODO add session_password
    def create_gds(self, session_name: str) -> GraphDataScience:
        create_details = self._aura_api.create_instance(session_name)

        gds_user = create_details.username
        gds_pw = create_details.password
        url = create_details.connection_url

        # FIXME we need to wait for the instance to be ready

        return self._construct_client(gds_url=url, gds_user=gds_user, gds_pw=gds_pw)

    def delete_gds(self, session_name: str) -> None:
        self._aura_api.delete_instance(session_name)

    def list_sessions(self) -> List[SessionInfo]:
        all_instances = self._aura_api.list_instances()

        return [
            SessionInfo(instance.name)
            for instance in all_instances
            if instance.name.startswith(AuraSessions.GDS_SESSION_NAME_PREFIX)
        ]

    def _construct_client(self, gds_url: str, gds_user: str, gds_pw: str) -> GraphDataScience:
        return GraphDataScience(
            endpoint=gds_url, auth=(gds_user, gds_pw), aura_ds=True, aura_db_connection_info=self._db_credentials
        )

    @classmethod
    def _instance_name(cls: Type[AuraSessions], session_name: str) -> str:
        return f"{AuraSessions.GDS_SESSION_NAME_PREFIX}{session_name}"
