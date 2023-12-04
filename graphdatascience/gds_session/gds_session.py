# from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, Union

from graphdatascience.aura_api import AuraApi
from graphdatascience.aura_graph_data_science import AuraGraphDataScience
from graphdatascience.aura_sessions import AuraSessions, SessionInfo
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)


@dataclass
class DbmsConnectionInfo:
    uri: str
    username: str
    password: str

    def auth(self) -> Tuple[str, str]:
        return self.username, self.password


@dataclass
class AuraAPICredentials:
    client_id: str
    client_secret: str
    tenant: Optional[str]


class GdsSessions:
    def __init__(self, db_connection: DbmsConnectionInfo, ds_connection: Union[DbmsConnectionInfo, AuraAPICredentials]):
        self._db_credentials = db_connection
        self._ds_credentials = ds_connection
        if isinstance(self._ds_credentials, AuraAPICredentials):
            self._aura_api = AuraApi(
                tenant_id=self._ds_credentials.tenant,
                client_id=self._ds_credentials.client_id,
                client_secret=self._ds_credentials.client_secret,
            )
        else:
            pass

    def connect(self, session_name: str, size: str) -> AuraGraphDataScience:
        # todo: copy this correctly from aura_sessions.py
        db_instance_id = AuraApi.extract_id(self._db_credentials.uri)
        db_instance = self._aura_api.list_instance(db_instance_id)

        create_details = self._aura_api.create_instance(
            AuraSessions._instance_name(session_name), size, db_instance.cloud_provider, db_instance.region
        )

        if isinstance(self._ds_credentials, AuraAPICredentials):
            return AuraGraphDataScience(
                endpoint=create_details.connection_url,
                auth=(create_details.username, create_details.password),
                aura_db_connection_info=AuraDbConnectionInfo(
                    uri=self._db_credentials.uri, auth=self._db_credentials.auth()
                ),
            )
        else:
            pass

    def list_sessions(self) -> list[SessionInfo]:
        all_instances = self._aura_api.list_instances()

        return [
            SessionInfo(AuraSessions._session_name(instance))
            for instance in all_instances
            if instance.name.startswith(AuraSessions.GDS_SESSION_NAME_PREFIX)
        ]

    def disconnect(self, session_name: str) -> bool:
        return True


class GdsSessionBuilderWithDbAndDs:
    def __init__(
            self, db_credentials: DbmsConnectionInfo, ds_credentials: Union[DbmsConnectionInfo, AuraAPICredentials]
    ):
        self._ds_credentials = ds_credentials
        self._db_credentials = db_credentials

    def build(self) -> GdsSessions:
        return GdsSessions(self._db_credentials, self._ds_credentials)


class GdsSessionBuilderWithDs:
    def __init__(self, ds_credentials: Union[DbmsConnectionInfo, AuraAPICredentials]):
        self._ds_credentials = ds_credentials

    def db(self, db_credentials: DbmsConnectionInfo) -> GdsSessionBuilderWithDbAndDs:
        return GdsSessionBuilderWithDbAndDs(db_credentials, self._ds_credentials)


class GdsSessionBuilderWithDb:
    def __init__(self, db_credentials: DbmsConnectionInfo):
        self._db_credentials = db_credentials

    def ds(self, ds_credentials: Union[DbmsConnectionInfo, AuraAPICredentials]) -> GdsSessionBuilderWithDbAndDs:
        return GdsSessionBuilderWithDbAndDs(self._db_credentials, ds_credentials)


class GdsSessionBuilder:
    @staticmethod
    def db(db_credentials: DbmsConnectionInfo) -> GdsSessionBuilderWithDb:
        return GdsSessionBuilderWithDb(db_credentials)

    @staticmethod
    def ds(ds_credentials: Union[DbmsConnectionInfo, AuraAPICredentials]) -> GdsSessionBuilderWithDs:
        return GdsSessionBuilderWithDs(ds_credentials)


def builder():
    gds_sessions = (
        GdsSessionBuilder.db(DbmsConnectionInfo(uri="bolt://localhost:7687", username="neo4j", password="neo4j"))
        .ds(AuraAPICredentials(client_id="client_id", client_secret="client_secret", tenant="tenant"))
        .build()
    )

    gds_sessions.list_sessions()

    gds = gds_sessions.connect(session_name="test-session", size="8GB")
    G, result = gds.graph.project("g", "RETURN gds.graph.project(0, 1)")
    gds.pageRank.mutate(G, mutateProperty="pagerank")
    gds.graph.nodeProperty.stream(G, "pagerank")

    gds_sessions.disconnect("test-session")
    # alternatively
    gds.disconnect()


def namedparams():
    gds_sessions = GdsSessions(
        db_connection=DbmsConnectionInfo(uri="bolt://localhost:7687", username="neo4j", password="neo4j"),
        ds_connection=AuraAPICredentials(client_id="client_id", client_secret="client_secret", tenant="tenant"),
    )

    gds_sessions.list_sessions()

    gds = gds_sessions.connect(session_name="test-session", size="8GB")
    G, result = gds.graph.project("g", "RETURN gds.graph.project(0, 1)")
    gds.pageRank.mutate(G, mutateProperty="pagerank")
    gds.graph.nodeProperty.stream(G, "pagerank")

    gds_sessions.disconnect("test-session")
    # alternatively
    gds.disconnect()
