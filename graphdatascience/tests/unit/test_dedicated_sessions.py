import dataclasses
import re
from typing import List, Optional

import pytest
from pytest_mock import MockerFixture

from graphdatascience.session.aura_api import AuraApi
from graphdatascience.session.aura_api_responses import (
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    SessionDetails,
    TenantDetails,
    WaitResult,
)
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.dedicated_sessions import DedicatedSessions
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory


class FakeAuraApi(AuraApi):
    def __init__(
        self,
        existing_instances: Optional[List[InstanceSpecificDetails]] = None,
        existing_sessions: Optional[List[SessionDetails]] = None,
        status_after_creating: str = "Ready",
    ) -> None:
        super().__init__("", "", "tenant_id")
        if existing_instances is None:
            existing_instances = []
        if existing_sessions is None:
            existing_sessions = []
        self._instances = {details.id: details for details in existing_instances}
        self._sessions = {details.id: details for details in existing_sessions}
        self.id_counter = 0
        self.time = 0
        self._status_after_creating = status_after_creating

    def create_session(self, name: str, dbid: str, pwd: str) -> SessionDetails:
        details = SessionDetails(
            id=f"{dbid}-ffff{self.id_counter}",
            name=name,
            instance_id=dbid,
            memory="2GB",
            status="Creating",
            created_at="some-date",
            host="foo.bar",
            expiry_date=None,
        )

        self.id_counter += 1
        self._sessions[details.id] = details

        return details

    def create_instance(self, name: str, memory: str, cloud_provider: str, region: str) -> InstanceCreateDetails:
        id = f"ffff{self.id_counter}"
        create_details = InstanceCreateDetails(
            id=id,
            username="neo4j",
            password="fake-pw",
            connection_url=f"neo4j+s://{id}.neo4j.io",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            status="creating",
            connection_url=create_details.connection_url,
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

    def delete_session(self, session_id: str, dbid: str) -> bool:
        return not self._sessions.pop(session_id, None) is None

    def delete_instance(self, instance_id: str) -> InstanceSpecificDetails:
        return self._instances.pop(instance_id)

    def list_sessions(self, dbid: str) -> List[SessionDetails]:
        return [v for _, v in self._sessions.items() if v.instance_id == dbid]

    def list_instances(self) -> List[InstanceDetails]:
        return [v for _, v in self._instances.items()]

    def list_session(self, session_id: str, dbid: str) -> Optional[SessionDetails]:
        matched_instance = self._sessions.get(session_id, None)

        if matched_instance:
            old_instance = matched_instance
            self._sessions[session_id] = dataclasses.replace(old_instance, status=self._status_after_creating)
            return old_instance
        else:
            return None

    def list_instance(self, instance_id: str) -> Optional[InstanceSpecificDetails]:
        matched_instances = self._instances.get(instance_id, None)

        if matched_instances:
            old_instance = matched_instances
            self._instances[instance_id] = dataclasses.replace(old_instance, status=self._status_after_creating)
            return old_instance
        else:
            return None

    def wait_for_session_running(
        self, session_id: str, dbid: str, sleep_time: float = 0.2, max_sleep_time: float = 5
    ) -> WaitResult:
        return super().wait_for_session_running(session_id, dbid, sleep_time=0.0001, max_sleep_time=0.001)

    def wait_for_instance_running(
        self, instance_id: str, sleep_time: float = 0.2, max_sleep_time: float = 300
    ) -> WaitResult:
        return super().wait_for_instance_running(instance_id, sleep_time=0.0001, max_sleep_time=0.001)

    def tenant_details(self) -> TenantDetails:
        return TenantDetails(id=self._tenant_id, ds_type="fake-ds", regions_per_provider={"aws": {"leipzig-1"}})


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi()


def test_list_session(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)
    session = aura_api.create_session(
        name="gds-session-my-session-name", dbid=aura_api.list_instances()[0].id, pwd="some_pwd"
    )
    sessions = DedicatedSessions(aura_api)

    assert sessions.list() == [SessionInfo.from_session_details(session)]


def test_create_session(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = DedicatedSessions(aura_api)

    patch_construct_client(mocker)

    gds_credentials = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_credentials == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "neo4j://foo.bar",
        "session_name": "my-session",
    }
    assert [i.name for i in sessions.list()] == ["my-session"]


def test_get_or_create(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = DedicatedSessions(aura_api)

    patch_construct_client(mocker)

    gds_args1 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    gds_args2 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_args1 == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+ssc://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "gds_url": "neo4j://foo.bar",
        "session_name": "my-session",
    }
    assert gds_args1 == gds_args2

    assert [i.name for i in sessions.list()] == ["my-session"]


def test_get_or_create_duplicate_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)
    aura_api.create_session("one", db.id, "1234")
    aura_api.create_session("one", db.id, "12345")

    sessions = DedicatedSessions(aura_api)

    with pytest.raises(RuntimeError, match=re.escape("Expected to find exactly one GDS session with name `one`")):
        sessions.get_or_create("one", SessionMemory.m_8GB, DbmsConnectionInfo(db.connection_url, "", ""))


def test_delete_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", "1GB", "aura", "leipzig").id
    db2 = aura_api.create_instance("db2", "1GB", "aura", "dresden").id
    aura_api.create_session("one", db1, "12345")
    aura_api.create_session("other", db2, "123123")

    sessions = DedicatedSessions(aura_api)

    assert sessions.delete("one")
    assert [i.name for i in sessions.list()] == ["other"]


def test_delete_nonexisting_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", "1gb", "aura", "leipzig").id
    aura_api.create_session("one", db1, "12345")
    sessions = DedicatedSessions(aura_api)

    assert sessions.delete("other") is False
    assert [i.name for i in sessions.list()] == ["one"]


def test_delete_nonunique_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", "1GB", "aura", "leipzig").id
    aura_api.create_session("one", db1, "12345")
    aura_api.create_session("one", db1, "12345")
    sessions = DedicatedSessions(aura_api)

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Expected to find exactly one GDS session with name `one`," " but found `['ffff0-ffff1', 'ffff0-ffff2']`."
        ),
    ):
        sessions.delete("one")

    assert len(sessions.list()) == 2


def test_create_waiting_forever() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    _setup_db_instance(aura_api)
    sessions = DedicatedSessions(aura_api)

    with pytest.raises(RuntimeError, match="Failed to create session `one`: Session is not running after waiting"):
        sessions.get_or_create(
            "one", SessionMemory.m_8GB, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def _setup_db_instance(aura_api: AuraApi) -> InstanceCreateDetails:
    return aura_api.create_instance("test", "8GB", "aws", "leipzig-1")


def patch_construct_client(mocker: MockerFixture) -> None:
    mocker.patch(
        "graphdatascience.session.dedicated_sessions.DedicatedSessions._construct_client",
        lambda *args, **kwargs: kwargs,
    )
