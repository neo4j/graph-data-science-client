import dataclasses
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, cast

import pytest
from pytest_mock import MockerFixture

from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi, AuraApiError
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    SessionDetails,
    TenantDetails,
    WaitResult,
)
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.dedicated_sessions import DedicatedSessions
from graphdatascience.session.session_info import ExtendedSessionInfo, SessionInfo
from graphdatascience.session.session_sizes import SessionMemory, SessionMemoryValue


class FakeAuraApi(AuraApi):
    def __init__(
        self,
        existing_instances: Optional[List[InstanceSpecificDetails]] = None,
        existing_sessions: Optional[List[SessionDetails]] = None,
        status_after_creating: str = "Ready",
        size_estimation: Optional[EstimationDetails] = None,
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
        self._size_estimation = size_estimation or EstimationDetails("1GB", "8GB", False)

    def create_session(self, name: str, dbid: str, pwd: str, memory: SessionMemoryValue) -> SessionDetails:
        details = SessionDetails(
            id=f"{dbid}-ffff{self.id_counter}",
            name=name,
            instance_id=dbid,
            memory=memory,
            status="Creating",
            created_at=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
            host="foo.bar",
            expiry_date=None,
            ttl=None,
            user_id="user-1",
            tenant_id=self._tenant_id,
        )

        self.id_counter += 1
        self._sessions[details.id] = details

        return details

    def add_session(self, session: SessionDetails) -> None:
        if session.id in self._sessions:
            raise ValueError(f"Session with id {session.id} already exists.")

        self._sessions[session.id] = session

    # aura behaviour of paused instances not being in an orchestra
    def _mimic_paused_db_behaviour(self, dbid: str) -> None:
        db = self.list_instance(dbid)
        if db and db.status == "paused":
            raise AuraApiError(message="Database not found", status_code=404)

    def create_instance(
        self, name: str, memory: SessionMemoryValue, cloud_provider: str, region: str
    ) -> InstanceCreateDetails:
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
        self._mimic_paused_db_behaviour(dbid)

        return [v for _, v in self._sessions.items() if v.instance_id == dbid]

    def list_instances(self) -> List[InstanceDetails]:
        return [v for _, v in self._instances.items()]

    def list_session(self, session_id: str, dbid: str) -> Optional[SessionDetails]:
        self._mimic_paused_db_behaviour(dbid)

        matched_session = self._sessions.get(session_id, None)

        if matched_session:
            old_session = matched_session
            self._sessions[session_id] = dataclasses.replace(old_session, status=self._status_after_creating)
            return old_session
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
        self,
        session_id: str,
        dbid: str,
        sleep_time: float = 0.2,
        max_sleep_time: float = 5,
        max_wait_time: float = 5,
    ) -> WaitResult:
        return super().wait_for_session_running(
            session_id, dbid, sleep_time=0.0001, max_wait_time=0.001, max_sleep_time=0.001
        )

    def wait_for_instance_running(
        self,
        instance_id: str,
        sleep_time: float = 0.2,
        max_sleep_time: float = 5,
        max_wait_time: float = 5,
    ) -> WaitResult:
        return super().wait_for_instance_running(
            instance_id, sleep_time=0.0001, max_wait_time=0.001, max_sleep_time=0.001
        )

    def tenant_details(self) -> TenantDetails:
        return TenantDetails(id=self._tenant_id, ds_type="fake-ds", regions_per_provider={"aws": {"leipzig-1"}})

    def estimate_size(
        self, node_count: int, relationship_count: int, algorithm_categories: List[AlgorithmCategory]
    ) -> EstimationDetails:
        return self._size_estimation


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi()


HASHED_DB_PASSWORD = "722cbc618c015c7c062f071868d9bb5f207f35a317e71054740716642cfd0f61"


def test_list_session(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)
    session = aura_api.create_session(
        name="gds-session-my-session-name",
        dbid=aura_api.list_instances()[0].id,
        pwd="some_pwd",
        memory=SessionMemory.m_8GB.value,
    )
    sessions = DedicatedSessions(aura_api)

    assert sessions.list() == [SessionInfo.from_session_details(session)]


def test_list_session_paused_instance(aura_api: AuraApi) -> None:
    db = aura_api.create_instance("test", SessionMemory.m_8GB.value, "aws", "leipzig-1")
    fake_aura_api = cast(FakeAuraApi, aura_api)

    fake_aura_api.id_counter += 1
    paused_db = InstanceSpecificDetails(
        id="4242",
        status="paused",
        connection_url="foo.bar",
        memory=SessionMemory.m_16GB.value,
        type="",
        region="dresden",
        name="paused-db",
        tenant_id=fake_aura_api._tenant_id,
        cloud_provider="aws",
    )
    fake_aura_api._instances[paused_db.id] = paused_db

    session = aura_api.create_session(
        name="gds-session-my-session-name",
        dbid=db.id,
        pwd="some_pwd",
        memory=SessionMemory.m_8GB.value,
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
        DbmsConnectionInfo("neo4j+s://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_credentials == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+s://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "session_connection": DbmsConnectionInfo(
            uri="neo4j+s://foo.bar", username="neo4j", password=HASHED_DB_PASSWORD
        ),
        "session_id": "ffff0-ffff1",
    }

    assert len(sessions.list()) == 1
    actual_session = sessions.list()[0]

    assert isinstance(actual_session, ExtendedSessionInfo)
    assert actual_session.name == "my-session"
    assert actual_session.user_id == "user-1"


def test_get_or_create(mocker: MockerFixture, aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = DedicatedSessions(aura_api)

    patch_construct_client(mocker)

    gds_args1 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+s://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )
    gds_args2 = sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo("neo4j+s://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
    )

    assert gds_args1 == {  # type: ignore
        "db_connection": DbmsConnectionInfo(
            uri="neo4j+s://ffff0.databases.neo4j.io", username="dbuser", password="db_pw"
        ),
        "session_connection": DbmsConnectionInfo(
            uri="neo4j+s://foo.bar", username="neo4j", password=HASHED_DB_PASSWORD
        ),
        "session_id": "ffff0-ffff1",
    }
    assert gds_args1 == gds_args2

    assert [i.name for i in sessions.list()] == ["my-session"]


def test_get_or_create_duplicate_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)
    aura_api.create_session("one", db.id, "1234", memory=SessionMemory.m_4GB.value)
    aura_api.create_session("one", db.id, "12345", memory=SessionMemory.m_4GB.value)

    sessions = DedicatedSessions(aura_api)

    with pytest.raises(RuntimeError, match=re.escape("Expected to find exactly one GDS session with name `one`")):
        sessions.get_or_create("one", SessionMemory.m_8GB, DbmsConnectionInfo(db.connection_url, "", ""))


def test_get_or_create_expired_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetails(
            id="ffff0-ffff1",
            name="one",
            instance_id=db.id,
            memory=SessionMemory.m_8GB.value,
            status="Expired",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=None,
            ttl=None,
            tenant_id="tenant-0",
            user_id="user-0",
        )
    )

    with pytest.raises(
        RuntimeError, match=re.escape("Session `one` is expired. Please delete it and create a new one.")
    ):
        sessions = DedicatedSessions(aura_api)
        sessions.get_or_create("one", SessionMemory.m_8GB, DbmsConnectionInfo(db.connection_url, "", ""))


def test_get_or_create_soon_expired_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetails(
            id="ffff0-ffff1",
            name="one",
            instance_id=db.id,
            memory=SessionMemory.m_8GB.value,
            status="Ready",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=datetime.now(tz=timezone.utc) - timedelta(hours=23),
            ttl=None,
            tenant_id="tenant-0",
            user_id="user-0",
        )
    )

    with pytest.raises(Warning, match=re.escape("Session `one` is expiring in less than a day.")):
        sessions = DedicatedSessions(aura_api)
        sessions.get_or_create("one", SessionMemory.m_8GB, DbmsConnectionInfo(db.connection_url, "", ""))


def test_get_or_create_with_different_memory_config(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetails(
            id="ffff0-ffff1",
            name="one",
            instance_id=db.id,
            memory=SessionMemory.m_8GB.value,
            status="Ready",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=None,
            ttl=None,
            tenant_id="tenant-0",
            user_id="user-0",
        )
    )

    with pytest.raises(
        RuntimeError,
        match=re.escape("Session `one` exists with a different memory configuration. Current: 8GB, Requested: 16GB."),
    ):
        sessions = DedicatedSessions(aura_api)
        sessions.get_or_create("one", SessionMemory.m_16GB, DbmsConnectionInfo(db.connection_url, "", ""))


def test_delete_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", SessionMemory.m_4GB.value, "aura", "leipzig").id
    db2 = aura_api.create_instance("db2", SessionMemory.m_4GB.value, "aura", "dresden").id
    aura_api.create_session("one", db1, "12345", memory=SessionMemory.m_8GB.value)
    aura_api.create_session("other", db2, "123123", memory=SessionMemory.m_8GB.value)

    sessions = DedicatedSessions(aura_api)

    assert sessions.delete("one")
    assert [i.name for i in sessions.list()] == ["other"]


def test_delete_nonexisting_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", SessionMemory.m_4GB.value, "aura", "leipzig").id
    aura_api.create_session("one", db1, "12345", memory=SessionMemory.m_8GB.value)
    sessions = DedicatedSessions(aura_api)

    assert sessions.delete("other") is False
    assert [i.name for i in sessions.list()] == ["one"]


def test_delete_nonunique_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", SessionMemory.m_4GB.value, "aura", "leipzig").id
    aura_api.create_session("one", db1, "12345", memory=SessionMemory.m_8GB.value)
    aura_api.create_session("one", db1, "12345", memory=SessionMemory.m_8GB.value)
    sessions = DedicatedSessions(aura_api)

    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "Expected to find exactly one GDS session with name `one`," " but found `['ffff0-ffff1', 'ffff0-ffff2']`."
        ),
    ):
        sessions.delete("one")

    assert len(sessions.list()) == 2


def test_delete_session_paused_instance(aura_api: AuraApi) -> None:
    fake_aura_api = cast(FakeAuraApi, aura_api)

    fake_aura_api.id_counter += 1
    paused_db = InstanceSpecificDetails(
        id="4242",
        status="paused",
        connection_url="foo.bar",
        memory=SessionMemory.m_16GB.value,
        type="",
        region="dresden",
        name="paused-db",
        tenant_id=fake_aura_api._tenant_id,
        cloud_provider="aws",
    )
    fake_aura_api._instances[paused_db.id] = paused_db

    session = aura_api.create_session(
        name="gds-session-my-session-name",
        dbid=paused_db.id,
        pwd="some_pwd",
        memory=SessionMemory.m_8GB.value,
    )
    sessions = DedicatedSessions(aura_api)

    # cannot delete session running against a paused instance
    assert not sessions.delete(session.name)


def test_create_waiting_forever() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    _setup_db_instance(aura_api)
    sessions = DedicatedSessions(aura_api)

    with pytest.raises(
        RuntimeError, match="Failed to create session `one`: Session `ffff0-ffff1` for database `ffff0` is not running"
    ):
        sessions.get_or_create(
            "one", SessionMemory.m_8GB, DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", "")
        )


def test_estimate_size() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("1GB", "8GB", False))
    sessions = DedicatedSessions(aura_api)

    assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def test_estimate_size_exceeds() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("16GB", "8GB", True))
    sessions = DedicatedSessions(aura_api)

    with pytest.warns(
        ResourceWarning,
        match=re.escape("The estimated memory `16GB` exceeds the maximum size supported by your Aura tenant (`8GB`)"),
    ):
        assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def _setup_db_instance(aura_api: AuraApi) -> InstanceCreateDetails:
    return aura_api.create_instance("test", SessionMemory.m_8GB.value, "aws", "leipzig-1")


def patch_construct_client(mocker: MockerFixture) -> None:
    mocker.patch(
        "graphdatascience.session.dedicated_sessions.DedicatedSessions._construct_client",
        lambda *args, **kwargs: kwargs,
    )
