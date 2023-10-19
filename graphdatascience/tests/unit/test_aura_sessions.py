import dataclasses
import re
from dataclasses import asdict
from typing import Any, List, Optional

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker

from graphdatascience.aura_api import (
    AuraApi,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
)
from graphdatascience.aura_sessions import AuraSessions, SessionInfo
from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.aura_db_arrow_query_runner import (
    AuraDbConnectionInfo,
)


class FakeAuraApi(AuraApi):
    def __init__(self, existing_instances: Optional[List[InstanceSpecificDetails]] = None) -> None:
        super().__init__("", "", "tenant_id")
        if existing_instances is None:
            existing_instances = []
        self._instances = {details.id: details for details in existing_instances}
        self.id_counter = 0
        self.time = 0

    def create_instance(self, name: str) -> InstanceCreateDetails:
        create_details = InstanceCreateDetails(
            id=f"id-{self.id_counter}",
            name=name,
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            username="neo4j",
            password="fake-pw",
            connection_url="fake-url",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            name=create_details.name,
            tenant_id=create_details.tenant_id,
            cloud_provider=create_details.cloud_provider,
            status="creating",
            connection_url="fake-url",
            memory="",
        )

        self.id_counter += 1
        self._instances[specific_details.id] = specific_details

        return create_details

    def delete_instance(self, instance_id: str) -> InstanceSpecificDetails:
        return self._instances.pop(instance_id)

    def list_instances(self) -> List[InstanceDetails]:
        return [v for _, v in self._instances.items()]

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        matched_instances = self._instances.get(instance_id, None)

        if matched_instances:
            old_instance = matched_instances
            self._instances[instance_id] = dataclasses.replace(old_instance, status="running")
            return old_instance
        else:
            return None

    def wait_for_instance_running(self, instance_id: str, sleep_time: float = 0.2) -> bool:
        return super().wait_for_instance_running(instance_id, 0.0001)


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi()


def test_list_session(requests_mock: Mocker) -> None:
    sessions = AuraSessions(
        db_credentials=AuraDbConnectionInfo("", ("", "")), aura_api_client_auth=("", ""), tenant_id="placeholder"
    )

    db_instance = InstanceDetails(id="id", name="Instance01", tenant_id="tenant_id", cloud_provider="cloud_provider")
    gds_instance = InstanceDetails(
        id="id", name="gds-session-my-session-name", tenant_id="tenant_id", cloud_provider="cloud_provider"
    )

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/instances", json={"data": [asdict(i) for i in [db_instance, gds_instance]]}
    )

    assert sessions.list_sessions() == [SessionInfo("my-session-name")]


def test_create_session(mocker: MockerFixture, gds: GraphDataScience, aura_api: AuraApi) -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")
    sessions._aura_api = aura_api

    def assert_db_credentials(*args: List[Any], **kwargs: dict[str, Any]) -> GraphDataScience:
        assert kwargs == {"gds_url": "fake-url", "gds_user": "neo4j", "initial_pw": "fake-pw", "new_pw": "my-password"}
        return gds

    mocker.patch("graphdatascience.aura_sessions.AuraSessions._construct_client",  lambda *args, **kwargs: kwargs)
    mocker.patch("graphdatascience.aura_sessions.AuraSessions._change_initial_pw", assert_db_credentials)

    gds_credentials = sessions.create_gds("my-session", "my-password")

    assert gds_credentials == {"gds_url": "fake-url", "gds_user": "neo4j", "gds_pw": "my-password"}
    assert sessions.list_sessions() == [SessionInfo("my-session")]


def test_create_duplicate_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")
    sessions._aura_api = aura_api

    mocker.patch("graphdatascience.aura_sessions.AuraSessions._construct_client", lambda *args, **kwargs: None)
    mocker.patch("graphdatascience.aura_sessions.AuraSessions._change_initial_pw", lambda *args, **kwargs: None)

    sessions.create_gds("my-session", "my-password")

    with pytest.raises(RuntimeError, match=re.escape("Session with name `my-session` already exists")):
        sessions.create_gds("my-session", "my-password")

    assert sessions.list_sessions() == [SessionInfo("my-session")]


def test_connect_to_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")
    sessions._aura_api = aura_api

    mocker.patch("graphdatascience.aura_sessions.AuraSessions._construct_client", lambda *args, **kwargs: kwargs)
    mocker.patch("graphdatascience.aura_sessions.AuraSessions._change_initial_pw", lambda *args, **kwargs: None)

    sessions.create_gds("my-session", "my-password")
    gds_args = sessions.connect("my-session", "my-password")

    assert gds_args == {"gds_pw": "my-password", "gds_url": "fake-url", "gds_user": "neo4j"}


def test_delete_session() -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=AuraSessions._instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
        ),
        InstanceSpecificDetails(
            id="id1",
            name=AuraSessions._instance_name("other"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    assert sessions.delete_gds("one")
    assert sessions.list_sessions() == [SessionInfo("other")]


def test_delete_nonexisting_session() -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=AuraSessions._instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    assert sessions.delete_gds("other") is False
    assert sessions.list_sessions() == [SessionInfo("one")]


def test_delete_nonunique_session() -> None:
    db_credentials = AuraDbConnectionInfo("db-uri", ("dbuser", "db_pw"))
    sessions = AuraSessions(db_credentials, aura_api_client_auth=("", ""), tenant_id="placeholder")

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=AuraSessions._instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
        ),
        InstanceSpecificDetails(
            id="id43",
            name=AuraSessions._instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Expected to find exactly one instance with name `one`, but found `[('id42', 'one'), ('id43', 'one')]`"
        ),
    ):
        sessions.delete_gds("one")

    assert sessions.list_sessions() == [SessionInfo("one"), SessionInfo("one")]
