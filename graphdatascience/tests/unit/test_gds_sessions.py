import dataclasses
import re
from dataclasses import asdict
from typing import Any, Dict, List, Optional

import pytest
from pytest_mock import MockerFixture
from requests_mock import Mocker

from graphdatascience.gds_session.aura_api import (
    AuraApi,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    TenantDetails,
    WaitResult,
)
from graphdatascience.gds_session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.gds_session.gds_sessions import (
    AuraAPICredentials,
    GdsSessions,
    SessionInfo,
)
from graphdatascience.gds_session.session_sizes import SessionSizeByMemory, SessionSizes


class FakeAuraApi(AuraApi):
    def __init__(
        self, existing_instances: Optional[List[InstanceSpecificDetails]] = None, status_after_creating: str = "running"
    ) -> None:
        super().__init__("", "", "tenant_id")
        if existing_instances is None:
            existing_instances = []
        self._instances = {details.id: details for details in existing_instances}
        self.id_counter = 0
        self.time = 0
        self._status_after_creating = status_after_creating

    def create_instance(self, name: str, memory: str, cloud_provider: str, region: str) -> InstanceCreateDetails:
        create_details = InstanceCreateDetails(
            id=f"ffff{self.id_counter}",
            username="neo4j",
            password="fake-pw",
            connection_url="fake-url",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            status="creating",
            connection_url="fake-url",
            memory=memory,
            type="",
            region=region,
            name=name,
            tenant_id=self._tenant_id,
            cloud_provider=cloud_provider,
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
            self._instances[instance_id] = dataclasses.replace(old_instance, status=self._status_after_creating)
            return old_instance
        else:
            return None

    def wait_for_instance_running(
        self, instance_id: str, sleep_time: float = 0.2, max_sleep_time: float = 300
    ) -> WaitResult:
        return super().wait_for_instance_running(instance_id, sleep_time=0.0001, max_sleep_time=0.001)

    def tenant_details(self) -> TenantDetails:
        return TenantDetails(
            id=self._tenant_id, ds_type="fake-ds", regions_per_provider={"aws": {"leipzig-1", "dresden-2"}}
        )


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi()


def test_list_session(requests_mock: Mocker) -> None:
    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))

    db_instance = InstanceDetails(id="id", name="Instance01", tenant_id="tenant_id", cloud_provider="cloud_provider")
    gds_instance = InstanceDetails(
        id="id", name="gds-session-my-session-name", tenant_id="tenant_id", cloud_provider="cloud_provider"
    )
    gds_instance_specific_details = InstanceSpecificDetails(
        id="id",
        name="gds-session-my-session-name",
        tenant_id="tenant_id",
        cloud_provider="gcp",
        status="RUNNING",
        connection_url="",
        memory="16 GiB",
        type="gds",
        region="",
    )

    requests_mock.post(
        "https://api.neo4j.io/oauth/token",
        json={"access_token": "my_token", "expires_in": 3600, "token_type": "Bearer"},
    )
    requests_mock.get(
        "https://api.neo4j.io/v1/instances", json={"data": [asdict(i) for i in [db_instance, gds_instance]]}
    )
    requests_mock.get("https://api.neo4j.io/v1/instances/id", json={"data": asdict(gds_instance_specific_details)})

    assert sessions.list() == [SessionInfo("my-session-name", "16 GiB", "gds")]


def test_create_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = aura_api

    def assert_db_credentials(*args: List[Any], **kwargs: Dict[str, Any]) -> None:
        assert kwargs == {"gds_url": "fake-url", "gds_user": "neo4j", "initial_pw": "fake-pw", "new_pw": "db_pw"}

    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch("graphdatascience.gds_session.gds_sessions.GdsSessions._change_initial_pw", assert_db_credentials)

    gds_credentials = sessions.get_or_create(
        "my-session",
        SessionSizes.by_memory().X5L,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_credentials == {
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "fake-url",
        "session_name": "my-session",
    }
    assert sessions.list() == [SessionInfo("my-session", "512GB", "")]

    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "leipzig-1"
    assert instance_details.memory == "512GB"


def test_create_default_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = aura_api

    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._change_initial_pw", lambda *args, **kwargs: kwargs
    )

    sessions.get_or_create(
        "my-session",
        SessionSizeByMemory.DEFAULT,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "leipzig-1"
    assert instance_details.memory == "8GB"


def test_create_session_override_region(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = aura_api

    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._change_initial_pw", lambda *args, **kwargs: kwargs
    )

    sessions.get_or_create(
        "my-session",
        SessionSizeByMemory.DEFAULT,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
        region="dresden-2",
    )
    instance_details: InstanceSpecificDetails = aura_api.list_instance("ffff1")  # type: ignore
    assert instance_details.cloud_provider == "aws"
    assert instance_details.region == "dresden-2"
    assert instance_details.memory == "8GB"


def test_get_or_create(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = aura_api

    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._construct_client", lambda *args, **kwargs: kwargs
    )
    mocker.patch(
        "graphdatascience.gds_session.gds_sessions.GdsSessions._change_initial_pw", lambda *args, **kwargs: None
    )

    gds_args1 = sessions.get_or_create(
        "my-session",
        SessionSizeByMemory.DEFAULT,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    gds_args2 = sessions.get_or_create(
        "my-session",
        SessionSizeByMemory.DEFAULT,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_args1 == {
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "fake-url",
        "session_name": "my-session",
    }
    assert gds_args1 == gds_args2

    assert sessions.list() == [SessionInfo("my-session", "8GB", "")]


def test_get_or_create_duplicate_session() -> None:
    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = FakeAuraApi(
        existing_instances=[
            InstanceSpecificDetails(
                id="id42",
                name=GdsSessions._instance_name("one"),
                tenant_id="",
                cloud_provider="",
                status="RUNNING",
                connection_url="",
                memory="",
                type="",
                region="",
            ),
            InstanceSpecificDetails(
                id="id43",
                name=GdsSessions._instance_name("one"),
                tenant_id="",
                cloud_provider="",
                status="RUNNING",
                connection_url="",
                memory="",
                type="",
                region="",
            ),
        ]
    )

    with pytest.raises(RuntimeError, match=re.escape("Expected to find exactly one GDS session with name `one`")):
        sessions.get_or_create("one", SessionSizeByMemory.DEFAULT, DbmsConnectionInfo("", "", ""))


def test_delete_session() -> None:
    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=GdsSessions._instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
        InstanceSpecificDetails(
            id="id1",
            name=GdsSessions._instance_name("other"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    assert sessions.delete("one")
    assert sessions.list() == [SessionInfo("other", "", "")]


def test_delete_nonexisting_session() -> None:
    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=GdsSessions._instance_name("one"),
            tenant_id="tenant_id",
            cloud_provider="cloud_provider",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    assert sessions.delete("other") is False
    assert sessions.list() == [SessionInfo("one", "", "")]


def test_delete_nonunique_session() -> None:
    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))

    existing_instances = [
        InstanceSpecificDetails(
            id="id42",
            name=GdsSessions._instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
        InstanceSpecificDetails(
            id="id43",
            name=GdsSessions._instance_name("one"),
            tenant_id="",
            cloud_provider="",
            status="RUNNING",
            connection_url="",
            memory="",
            type="",
            region="",
        ),
    ]

    sessions._aura_api = FakeAuraApi(existing_instances=existing_instances)

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Expected to find exactly one GDS session with name `one`,"
            " but found `[('id42', 'one'), ('id43', 'one')]`."
        ),
    ):
        sessions.delete("one")

    assert sessions.list() == [SessionInfo("one", "", ""), SessionInfo("one", "", "")]


def test_create_immediate_delete(aura_api: AuraApi) -> None:
    aura_api = FakeAuraApi(status_after_creating="deleting")
    _setup_db_instance(aura_api)
    sessions = GdsSessions(AuraAPICredentials("", "", "foo"))
    sessions._aura_api = aura_api

    with pytest.raises(RuntimeError, match="Failed to create session `one`: Instance is being deleted"):
        sessions.get_or_create(
            "one", SessionSizeByMemory.DEFAULT, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def test_create_waiting_forever() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    _setup_db_instance(aura_api)
    sessions = GdsSessions(AuraAPICredentials("", "", "foo"))
    sessions._aura_api = aura_api

    with pytest.raises(RuntimeError, match="Failed to create session `one`: Instance is not running after waiting"):
        sessions.get_or_create(
            "one", SessionSizeByMemory.DEFAULT, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def test_create_session_invalid_region(aura_api: AuraApi) -> None:
    aura_api.create_instance("test", "8GB", "aws", "only-db-region")

    sessions = GdsSessions(AuraAPICredentials("", "", "placeholder"))
    sessions._aura_api = aura_api

    expected_message = "Region `only-db-region` is not supported by the tenant `tenant_id`." " Supported regions:"
    with pytest.raises(ValueError, match=expected_message):
        sessions.get_or_create(
            "my-session",
            SessionSizes.by_memory().X5L,
            DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
        )


def _setup_db_instance(aura_api: AuraApi) -> None:
    aura_api.create_instance("test", "8GB", "aws", "leipzig-1")
