import dataclasses
import re
from datetime import datetime, timedelta, timezone
from typing import Any, cast
from unittest import mock

import pytest
from pytest_mock import MockerFixture

from graphdatascience import GdsSessions
from graphdatascience.arrow_client.arrow_authentication import ArrowAuthentication
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session import AuraGraphDataScience
from graphdatascience.session.algorithm_category import AlgorithmCategory
from graphdatascience.session.aura_api import AuraApi, AuraApiError, SessionStatusError
from graphdatascience.session.aura_api_responses import (
    EstimationDetails,
    InstanceCreateDetails,
    InstanceDetails,
    InstanceSpecificDetails,
    ProjectDetails,
    SessionDetails,
    SessionDetailsWithErrors,
    SessionErrorData,
    TimeParser,
    WaitResult,
)
from graphdatascience.session.aura_api_token_authentication import AuraApiTokenAuthentication
from graphdatascience.session.cloud_location import CloudLocation
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo
from graphdatascience.session.session_info import SessionInfo
from graphdatascience.session.session_sizes import SessionMemory, SessionMemoryValue


class FakeAuraApi(AuraApi):
    def __init__(
        self,
        existing_instances: list[InstanceSpecificDetails] | None = None,
        existing_sessions: list[SessionDetailsWithErrors] | None = None,
        status_after_creating: str = "Ready",
        size_estimation: EstimationDetails | None = None,
        client_id: str = "client_id",
        console_user: str = "user-1",
        admin_user: str = "",
    ) -> None:
        super().__init__(client_id=client_id, client_secret="client_secret", project_id="project_id", aura_env="test")
        if existing_instances is None:
            existing_instances = []
        if existing_sessions is None:
            existing_sessions = []
        self._instances = {details.id: details for details in existing_instances}
        self._sessions = {details.id: details for details in existing_sessions}
        self.id_counter = 0
        self.time = 0
        self._status_after_creating = status_after_creating
        self._size_estimation = size_estimation or EstimationDetails("1GB", "8GB")
        self._console_user = console_user
        self._admin_user = admin_user

    def get_or_create_session(
        self,
        name: str,
        memory: SessionMemoryValue,
        instance_id: str | None = None,
        database_id: str | None = None,
        ttl: timedelta | None = None,
        cloud_location: CloudLocation | None = None,
    ) -> SessionDetails:
        if not cloud_location and instance_id:
            instance_details = self.list_instance(instance_id)
            if instance_details:
                cloud_location = CloudLocation(instance_details.cloud_provider, instance_details.region)
            id_prefix = instance_id
        else:
            id_prefix = "selfmanaged"

        for s in self._sessions.values():
            if s.name == name:
                if (
                    s.memory == memory
                    and s.user_id == self._console_user
                    and (not instance_id or s.instance_id == instance_id)
                    and (not cloud_location or s.cloud_location == cloud_location)
                ):
                    if errors := s.errors:
                        raise SessionStatusError(errors)
                    return s
                else:
                    raise RuntimeError("Session exists with different config")

        details = SessionDetailsWithErrors(
            id=f"{id_prefix}-ffff{self.id_counter}",
            name=name,
            instance_id=instance_id,
            database_id=database_id,
            memory=memory,
            status="Creating",
            created_at=datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
            host="foo.bar",
            expiry_date=None,
            ttl=ttl,
            user_id=self.console_user_id(),
            project_id=self._project_id,
            # we derive the location from the db
            cloud_location=cloud_location if cloud_location else CloudLocation("default", "default"),
        )

        self.id_counter += 1
        self._sessions[details.id] = details

        return details

    def console_user_id(self) -> str:
        return self._console_user

    def add_session(self, session: SessionDetailsWithErrors) -> None:
        if session.id in self._sessions:
            raise ValueError(f"Session with id {session.id} already exists.")

        self._sessions[session.id] = session

    # aura behaviour of paused instances not being in an orchestra
    def _mimic_paused_db_behaviour(self, dbid: str) -> None:
        db = self.list_instance(dbid)
        if db and db.status == "paused":
            raise AuraApiError(message="Database not found", status_code=404)

        if db and db.type == "gds":
            raise AuraApiError(
                message=f"Database with id `{db.id}` of tier `{db.type}`, which is not supported by sessions.",
                status_code=400,
            )

    def create_instance(
        self, name: str, memory: SessionMemoryValue, cloud_provider: str, region: str, type: str = "dsenterprise"
    ) -> InstanceCreateDetails:
        id = f"ffff{self.id_counter}"
        create_details = InstanceCreateDetails(
            id=id,
            username="neo4j",
            password="fake-pw",
            connection_url=f"neo4j+s://{id}.databases.neo4j.io",
        )

        specific_details = InstanceSpecificDetails(
            id=create_details.id,
            status="creating",
            connection_url=create_details.connection_url,
            memory="",
            type="",
            region=region,
            name=name,
            project_id=self._project_id,
            cloud_provider=cloud_provider,
        )

        self.id_counter += 1
        self._instances[specific_details.id] = specific_details

        return create_details

    def delete_session(self, session_id: str) -> bool:
        return self._sessions.pop(session_id, None) is not None

    def delete_instance(self, instance_id: str) -> InstanceSpecificDetails:
        return self._instances.pop(instance_id)

    def list_sessions(
        self,
        instance_id: str | None = None,
        list_only_owned: bool = False,
        include_deleted: bool = False,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[SessionDetailsWithErrors]:
        if instance_id:
            self._mimic_paused_db_behaviour(instance_id)

        matching_sessions = [v for _, v in self._sessions.items() if not instance_id or v.instance_id == instance_id]

        if list_only_owned:
            matching_sessions = [v for v in matching_sessions if v.user_id == self._console_user or self._admin_user]

        if start_date:
            matching_sessions = [v for v in matching_sessions if v.created_at >= start_date]

        if end_date:
            matching_sessions = [v for v in matching_sessions if v.created_at <= end_date]

        if not include_deleted:
            matching_sessions = [v for v in matching_sessions if v.status != "deleted"]

        return matching_sessions

    def list_instances(self) -> list[InstanceDetails]:
        return [v for _, v in self._instances.items()]

    def get_session(self, session_id: str) -> SessionDetails | None:
        matched_session = self._sessions.get(session_id, None)

        if matched_session:
            old_session = matched_session
            self._sessions[session_id] = dataclasses.replace(old_session, status=self._status_after_creating)

            if errors := old_session.errors:
                raise SessionStatusError(errors)

            return old_session
        else:
            return None

    def list_instance(self, instance_id: str) -> InstanceSpecificDetails | None:
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
        sleep_time: float = 1.0,
        max_sleep_time: float = 5,
        max_wait_time: float = 5,
    ) -> WaitResult:
        return super().wait_for_session_running(
            session_id, sleep_time=0.0001, max_wait_time=0.001, max_sleep_time=0.001
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

    def project_details(self) -> ProjectDetails:
        return ProjectDetails(id=self._project_id, cloud_locations={CloudLocation("aws", "leipzig-1")})

    def estimate_size(
        self,
        node_count: int,
        node_label_count: int,
        node_property_count: int,
        relationship_count: int,
        relationship_property_count: int,
        algorithm_categories: list[AlgorithmCategory],
    ) -> EstimationDetails:
        return self._size_estimation


class FakeGdsSessions(GdsSessions):
    """
    Test double for GdsSessions that avoids real database connections.

    Overrides _create_db_runner to store its arguments and return a mock runner,
    and _construct_client to store its arguments and return a mock client.
    """

    def __init__(self, aura_api: AuraApi, hosted_in_aura: bool = True) -> None:
        # We are purposefully not calling super.init() to avoid creating an actual instance of AuraAPI
        self._aura_api = aura_api
        self._hosted_in_aura = hosted_in_aura
        self.create_db_runner_calls: list[dict[str, Any]] = []
        self.construct_client_calls: list[dict[str, Any]] = []

    def _create_db_runner(
        self, db_connection: DbmsConnectionInfo, config: dict[str, Any] | None = None
    ) -> Neo4jQueryRunner:
        self.create_db_runner_calls.append({"db_connection": db_connection, "config": config})
        return mock.MagicMock(spec=Neo4jQueryRunner)

    def _check_hosted_in_aura(self, db_runner: Neo4jQueryRunner) -> bool:
        return self._hosted_in_aura

    def _construct_client(
        self,
        session_id: str,
        session_host: str,
        session_port: int,
        arrow_authentication: ArrowAuthentication,
        db_runner: Neo4jQueryRunner | None,
        arrow_client_options: dict[str, Any] | None = None,
    ) -> AuraGraphDataScience:
        self.construct_client_calls.append(
            {
                "session_id": session_id,
                "session_host": session_host,
                "session_port": session_port,
                "arrow_authentication": arrow_authentication,
                "db_runner": db_runner,
                "arrow_client_options": arrow_client_options,
            }
        )
        return mock.MagicMock(spec=AuraGraphDataScience)


@pytest.fixture
def aura_api() -> AuraApi:
    return FakeAuraApi(client_id="client-id")


def test_list_session(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)
    session = aura_api.get_or_create_session(
        name="gds-session-my-session-name",
        instance_id=aura_api.list_instances()[0].id,
        memory=SessionMemory.m_8GB.value,
    )
    sessions = FakeGdsSessions(aura_api)

    assert sessions.list() == [SessionInfo.from_session_details(session)]


def test_list_session_paused_instance(aura_api: AuraApi) -> None:
    db = aura_api.create_instance("test", SessionMemory.m_8GB.value, "aws", "leipzig-1")
    fake_aura_api = cast(FakeAuraApi, aura_api)

    fake_aura_api.id_counter += 1
    paused_db = InstanceSpecificDetails(
        id="4242",
        status="paused",
        connection_url="foo.bar",
        memory="16GB",
        type="",
        region="dresden",
        name="paused-db",
        project_id=fake_aura_api._project_id,
        cloud_provider="aws",
    )
    fake_aura_api._instances[paused_db.id] = paused_db

    session = aura_api.get_or_create_session(
        name="gds-session-my-session-name",
        instance_id=db.id,
        memory=SessionMemory.m_8GB.value,
    )
    sessions = FakeGdsSessions(aura_api)

    assert sessions.list() == [SessionInfo.from_session_details(session)]


def test_list_session_failed_session(aura_api: AuraApi) -> None:
    fake_aura_api = cast(FakeAuraApi, aura_api)

    session_details = SessionDetailsWithErrors(
        id="id0",
        name="name-0",
        status="Failed",
        instance_id="",
        database_id=None,
        created_at=TimeParser.fromisoformat("1970-01-01T00:00:00Z"),
        host="1.2.3.4",
        memory=SessionMemory.m_4GB.value,
        expiry_date=TimeParser.fromisoformat("1977-01-01T00:00:00Z"),
        project_id=fake_aura_api._project_id,
        user_id=fake_aura_api._console_user,
        ttl=timedelta(seconds=42),
        errors=[
            SessionErrorData(
                reason="OutOfMemory", message="Session reached its memory limit. Create a larger instance."
            )
        ],
    )
    fake_aura_api.add_session(session_details)

    sessions = FakeGdsSessions(aura_api)

    actualSessions = sessions.list()
    assert actualSessions == [SessionInfo.from_session_details(session_details)]
    assert actualSessions[0].errors and len(actualSessions[0].errors) == 1


def test_list_session_gds_instance(aura_api: AuraApi) -> None:
    db = aura_api.create_instance("test", SessionMemory.m_8GB.value, "aws", "leipzig-1")
    fake_aura_api = cast(FakeAuraApi, aura_api)

    fake_aura_api.id_counter += 1
    gds_instance = InstanceSpecificDetails(
        id="4242",
        status="Creating",
        connection_url="foo.bar",
        memory="16GB",
        type="gds",
        region="dresden",
        name="ds instance",
        project_id=fake_aura_api._project_id,
        cloud_provider="aws",
    )
    fake_aura_api._instances[gds_instance.id] = gds_instance

    session = aura_api.get_or_create_session(
        name="gds-session-my-session-name",
        instance_id=db.id,
        memory=SessionMemory.m_8GB.value,
    )
    sessions = FakeGdsSessions(aura_api)

    assert sessions.list() == [SessionInfo.from_session_details(session)]


def test_list_session_forwards_filters(mocker: MockerFixture, aura_api: AuraApi) -> None:
    sessions = FakeGdsSessions(aura_api)
    list_sessions_spy = mocker.spy(aura_api, "list_sessions")
    start_date = datetime(2025, 1, 1, 10, 0, tzinfo=timezone.utc)
    end_date = datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc)

    sessions.list(
        instance_id="instance-1",
        list_only_owned=True,
        include_deleted=False,
        start_date=start_date,
        end_date=end_date,
    )

    list_sessions_spy.assert_called_once_with(
        instance_id="instance-1",
        list_only_owned=True,
        include_deleted=False,
        start_date=start_date,
        end_date=end_date,
    )


def test_create_attached_session(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = FakeGdsSessions(aura_api)

    ttl = timedelta(hours=42)
    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
        ttl=ttl,
    )

    db_call = sessions.create_db_runner_calls[0]
    db_conn = db_call["db_connection"]
    auth = db_conn.get_auth()
    assert (auth.principal, auth.credentials) == ("dbuser", "db_pw")
    assert db_conn.get_uri() == "neo4j+s://ffff0.databases.neo4j.io"
    assert db_conn.database is None
    assert db_call["config"] is None

    construct_call = sessions.construct_client_calls[0]
    assert isinstance(construct_call["arrow_authentication"], AuraApiTokenAuthentication)
    assert construct_call["session_id"] == "ffff0-ffff1"
    assert construct_call["session_host"] == "foo.bar"
    assert construct_call["arrow_client_options"] is None

    assert len(sessions.list()) == 1
    actual_session = sessions.list()[0]
    assert actual_session.name == "my-session"
    assert actual_session.user_id == "user-1"
    assert actual_session.ttl == ttl


def test_create_attached_session_with_only_uri(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = FakeGdsSessions(aura_api, hosted_in_aura=True)

    ttl = timedelta(hours=42)
    with pytest.warns(
        DeprecationWarning,
        match=re.escape("Deriving the Aura instance from the database URI is deprecated"),
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j+s://ffff0.databases.neo4j.io", "dbuser", "db_pw"),
            ttl=ttl,
        )

    db_call = sessions.create_db_runner_calls[0]
    db_conn = db_call["db_connection"]
    auth = db_conn.get_auth()
    assert (auth.principal, auth.credentials) == ("dbuser", "db_pw")
    assert db_conn.get_uri() == "neo4j+s://ffff0.databases.neo4j.io"
    assert db_conn.database is None
    assert db_call["config"] is None

    construct_call = sessions.construct_client_calls[0]
    assert isinstance(construct_call["arrow_authentication"], AuraApiTokenAuthentication)
    assert construct_call["session_id"] == "ffff0-ffff1"
    assert construct_call["session_host"] == "foo.bar"
    assert construct_call["arrow_client_options"] is None

    assert len(sessions.list()) == 1
    actual_session = sessions.list()[0]
    assert actual_session.name == "my-session"
    assert actual_session.user_id == "user-1"
    assert actual_session.ttl == ttl


def test_create_attached_session_passthrough_arrow_settings(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = FakeGdsSessions(aura_api)

    ttl = timedelta(hours=42)
    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
        ttl=ttl,
        arrow_client_options={"foo": "bar"},
    )

    db_call = sessions.create_db_runner_calls[0]
    db_conn = db_call["db_connection"]
    auth = db_conn.get_auth()
    assert (auth.principal, auth.credentials) == ("dbuser", "db_pw")
    assert db_conn.get_uri() == "neo4j+s://ffff0.databases.neo4j.io"
    assert db_conn.database is None
    assert db_call["config"] is None

    construct_call = sessions.construct_client_calls[0]
    assert isinstance(construct_call["arrow_authentication"], AuraApiTokenAuthentication)
    assert construct_call["session_id"] == "ffff0-ffff1"
    assert construct_call["session_host"] == "foo.bar"
    assert construct_call["arrow_client_options"] == {"foo": "bar"}

    assert len(sessions.list()) == 1
    actual_session = sessions.list()[0]
    assert actual_session.name == "my-session"
    assert actual_session.user_id == "user-1"
    assert actual_session.ttl == ttl


def test_create_standalone_session(aura_api: AuraApi) -> None:
    sessions = FakeGdsSessions(aura_api, hosted_in_aura=False)

    ttl = timedelta(hours=42)
    sessions.get_or_create(
        "my-session",
        "8GB",
        DbmsConnectionInfo("neo4j+s://foo.bar", "dbuser", "db_pw"),
        cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
        ttl=ttl,
    )

    db_call = sessions.create_db_runner_calls[0]
    db_conn = db_call["db_connection"]
    auth = db_conn.get_auth()
    assert (auth.principal, auth.credentials) == ("dbuser", "db_pw")
    assert db_conn.get_uri() == "neo4j+s://foo.bar"
    assert db_conn.database is None
    assert db_call["config"] is None

    construct_call = sessions.construct_client_calls[0]
    assert isinstance(construct_call["arrow_authentication"], AuraApiTokenAuthentication)
    assert construct_call["session_id"] == "selfmanaged-ffff0"
    assert construct_call["session_host"] == "foo.bar"
    assert construct_call["arrow_client_options"] is None

    assert len(sessions.list()) == 1
    actual_session = sessions.list()[0]
    assert actual_session.name == "my-session"
    assert actual_session.user_id == "user-1"
    assert actual_session.ttl == ttl


def test_get_or_create_existing_session(aura_api: AuraApi) -> None:
    _setup_db_instance(aura_api)

    sessions = FakeGdsSessions(aura_api)

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
    )
    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
    )

    assert len(sessions.construct_client_calls) == 2
    for construct_call in sessions.construct_client_calls:
        assert isinstance(construct_call["arrow_authentication"], AuraApiTokenAuthentication)
        assert construct_call["session_id"] == "ffff0-ffff1"
        assert construct_call["session_host"] == "foo.bar"
        assert construct_call["arrow_client_options"] is None

    for db_call in sessions.create_db_runner_calls:
        auth = db_call["db_connection"].get_auth()
        assert (auth.principal, auth.credentials) == ("dbuser", "db_pw")
        assert db_call["db_connection"].get_uri() == "neo4j+s://ffff0.databases.neo4j.io"

    assert [i.name for i in sessions.list()] == ["my-session"]


def test_get_or_create_with_explicit_aura_instance_id(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)
    sessions = FakeGdsSessions(aura_api)

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id=db.id),
        cloud_location=None,
    )


def test_get_or_create_with_multidb_aura_instance(aura_api: AuraApi) -> None:
    instance = _setup_db_instance(aura_api)
    sessions = FakeGdsSessions(aura_api)

    sessions.get_or_create(
        "my-session",
        SessionMemory.m_8GB,
        DbmsConnectionInfo(
            username="dbuser", password="db_pw", aura_instance_id=instance.id, aura_database_id="db-id-1"
        ),
        cloud_location=None,
    )
    session = [i for i in aura_api.list_sessions() if i.name == "my-session"][0]

    assert session.database_id == "db-id-1"


def test_get_or_create_expired_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetailsWithErrors(
            id="ffff0-ffff1",
            name="one",
            instance_id=db.id,
            database_id=None,
            memory=SessionMemory.m_8GB.value,
            status="Expired",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=None,
            ttl=None,
            project_id=aura_api._project_id,
            user_id="user-1",
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
            errors=[SessionErrorData("foo", "inactivity")],
        )
    )

    with pytest.raises(SessionStatusError, match=re.escape("Session is in an unhealthy state")):
        sessions = FakeGdsSessions(aura_api)
        sessions.get_or_create(
            "one",
            SessionMemory.m_8GB,
            DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
        )


def test_get_or_create_soon_expired_session(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetailsWithErrors(
            id="ffff0-ffff1",
            name="one",
            instance_id=db.id,
            database_id=None,
            memory=SessionMemory.m_8GB.value,
            status="Ready",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=datetime.now(tz=timezone.utc) - timedelta(hours=23),
            ttl=None,
            project_id=aura_api._project_id,
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
            user_id="user-1",
        )
    )

    with pytest.raises(Warning, match=re.escape("Session `one` is expiring in 59 minutes.")):
        sessions = FakeGdsSessions(aura_api)
        sessions.get_or_create(
            "one",
            SessionMemory.m_8GB,
            DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id="ffff0"),
        )


def test_get_or_create_for_auradb_with_cloud_location(aura_api: AuraApi) -> None:
    db = _setup_db_instance(aura_api)

    sessions = FakeGdsSessions(aura_api)

    with pytest.raises(
        ValueError, match=re.escape("cloud_location cannot be provided for sessions against an AuraDB.")
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_8GB,
            DbmsConnectionInfo(username="dbuser", password="db_pw", aura_instance_id=db.id),
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
        )


def test_get_or_create_for_without_cloud_location(aura_api: AuraApi) -> None:
    sessions = FakeGdsSessions(aura_api, hosted_in_aura=False)

    with pytest.raises(
        ValueError, match=re.escape("cloud_location must be provided for sessions not attached to an AuraDB.")
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j://localhost:7687", "dbuser", "db_pw"),
            cloud_location=None,
        )


def test_get_or_create_for_non_derivable_aura_instance_id(aura_api: AuraApi) -> None:
    sessions = FakeGdsSessions(aura_api, hosted_in_aura=True)

    with (
        pytest.raises(
            ValueError,
            match=re.escape(
                "Aura instance with id `06cba79f` could not be found. Please specify the `aura_instance_id` in the `db_connection` argument."
            ),
        ),
        pytest.warns(
            DeprecationWarning, match=re.escape("Deriving the Aura instance from the database URI is deprecated")
        ),
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j+s://06cba79f.databases.neo4j.io", "dbuser", "db_pw"),
            cloud_location=None,
        )


def test_get_or_create_for_non_accessible_aura_instance(aura_api: AuraApi) -> None:
    sessions = FakeGdsSessions(aura_api)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Aura instance with id `06cba79f` could not be found. Please verify that the instance id is correct and that you have access to the Aura instance."
        ),
    ):
        sessions.get_or_create(
            "my-session",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j+s://foo.bar", "dbuser", "db_pw", aura_instance_id="06cba79f"),
            cloud_location=None,
        )


def test_get_or_create_failed_session(aura_api: AuraApi) -> None:
    fake_aura_api = cast(FakeAuraApi, aura_api)
    fake_aura_api.add_session(
        SessionDetailsWithErrors(
            id="ffff0-ffff1",
            name="one",
            instance_id=None,
            database_id=None,
            memory=SessionMemory.m_8GB.value,
            status="Failed",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=None,
            ttl=None,
            project_id=aura_api._project_id,
            user_id="user-1",
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
            errors=[SessionErrorData(message="error", reason="reason")],
        )
    )

    db_connection = DbmsConnectionInfo("foo.bar", "", "")
    sessions = FakeGdsSessions(aura_api, hosted_in_aura=False)

    with pytest.raises(
        SessionStatusError,
        match=re.escape("Session is in an unhealthy state. Details: ['Reason: reason, Message: error']"),
    ):
        sessions.get_or_create(
            "one", SessionMemory.m_8GB, db_connection, cloud_location=CloudLocation("aws", "leipzig-1")
        )


def test_delete_session_by_name(aura_api: AuraApi) -> None:
    aura_api.get_or_create_session("one", memory=SessionMemory.m_8GB.value, instance_id="12345")
    aura_api.get_or_create_session("other", memory=SessionMemory.m_8GB.value, instance_id="123123")

    sessions = FakeGdsSessions(aura_api)

    assert sessions.delete(session_name="one")
    assert [i.name for i in sessions.list()] == ["other"]


def test_delete_session_by_name_admin() -> None:
    aura_api = FakeAuraApi(console_user="user-1", admin_user="user-1")
    aura_api.add_session(
        SessionDetailsWithErrors(
            id="ffff0-ffff1",
            name="one",
            instance_id="1234",
            database_id=None,
            memory=SessionMemory.m_8GB.value,
            status="Ready",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=datetime.now(tz=timezone.utc) - timedelta(hours=23),
            ttl=None,
            project_id=aura_api._project_id,
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
            user_id="user-0",
        )
    )

    aura_api.add_session(
        SessionDetailsWithErrors(
            id="ffff0-ffff2",
            name="one",
            instance_id="1234",
            database_id=None,
            memory=SessionMemory.m_8GB.value,
            status="Ready",
            created_at=datetime.now(),
            host="foo.bar",
            expiry_date=datetime.now(tz=timezone.utc) - timedelta(hours=23),
            ttl=None,
            project_id=aura_api._project_id,
            cloud_location=CloudLocation(region="leipzig-1", provider="aws"),
            user_id="user-1",
        )
    )

    sessions = FakeGdsSessions(aura_api)
    with pytest.raises(
        RuntimeError,
        match=re.escape(
            "The user has access to multiple session with the name `one`. Please specify the id of the session that should be deleted."
        ),
    ):
        sessions.delete(session_name="one")


def test_delete_session_by_id(aura_api: AuraApi) -> None:
    s1 = aura_api.get_or_create_session("one", memory=SessionMemory.m_8GB.value, instance_id="12345")
    s2 = aura_api.get_or_create_session("other", memory=SessionMemory.m_8GB.value, instance_id="123123")

    sessions = FakeGdsSessions(aura_api)
    assert sessions.delete(session_id=s1.id)
    assert [i.name for i in sessions.list()] == [s2.name]


def test_delete_nonexisting_session(aura_api: AuraApi) -> None:
    db1 = aura_api.create_instance("db1", SessionMemory.m_4GB.value, "aura", "leipzig").id
    aura_api.get_or_create_session("one", memory=SessionMemory.m_8GB.value, instance_id=db1)
    sessions = FakeGdsSessions(aura_api)

    assert sessions.delete(session_name="other") is False
    assert [i.name for i in sessions.list()] == ["one"]


def test_delete_session_paused_instance(aura_api: AuraApi) -> None:
    fake_aura_api = cast(FakeAuraApi, aura_api)

    fake_aura_api.id_counter += 1
    paused_db = InstanceSpecificDetails(
        id="4242",
        status="paused",
        connection_url="foo.bar",
        memory="16GB",
        type="",
        region="dresden",
        name="paused-db",
        project_id=fake_aura_api._project_id,
        cloud_provider="aws",
    )
    fake_aura_api._instances[paused_db.id] = paused_db

    session = aura_api.get_or_create_session(
        name="gds-session-my-session-name",
        instance_id=paused_db.id,
        memory=SessionMemory.m_8GB.value,
    )
    sessions = FakeGdsSessions(aura_api)

    # can delete session running against a paused instance
    assert sessions.delete(session_name=session.name)


def test_create_waiting_forever() -> None:
    aura_api = FakeAuraApi(status_after_creating="updating")
    sessions = FakeGdsSessions(aura_api, hosted_in_aura=False)

    with pytest.raises(
        RuntimeError, match="Failed to get or create session `one`: Session `selfmanaged-ffff0` is not running"
    ):
        sessions.get_or_create(
            "one",
            SessionMemory.m_8GB,
            DbmsConnectionInfo("neo4j+ssc://ffff0.databases.neo4j.io", "", ""),
            cloud_location=CloudLocation("aws", "leipzig-1"),
        )


def test_estimate_size() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("1GB", "8GB"))
    sessions = FakeGdsSessions(aura_api)

    assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def test_estimate_str_categories_size() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("1GB", "8GB"))
    sessions = FakeGdsSessions(aura_api)

    assert sessions.estimate(1, 1, ["centrality"]) == SessionMemory.m_8GB


def test_estimate_size_exceeds() -> None:
    aura_api = FakeAuraApi(size_estimation=EstimationDetails("16GB", "8GB"))
    sessions = FakeGdsSessions(aura_api)

    with pytest.warns(
        ResourceWarning,
        match=re.escape("The estimated memory `16GB` exceeds the maximum size supported by your Aura project (`8GB`)"),
    ):
        assert sessions.estimate(1, 1, [AlgorithmCategory.CENTRALITY]) == SessionMemory.m_8GB


def _setup_db_instance(aura_api: AuraApi) -> InstanceCreateDetails:
    return aura_api.create_instance("test", SessionMemory.m_8GB.value, "aws", "leipzig-1")
