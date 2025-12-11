import neo4j
import pytest

from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo


def test_dbms_connection_info_username_password() -> None:
    dci = DbmsConnectionInfo(
        uri="foo.bar",
        username="neo4j",
        password="password",
        database="neo4j",
    )

    auth = dci.get_auth()

    assert (auth.principal, auth.credentials) == ("neo4j", "password")  # type:ignore


def test_dbms_connection_info_advanced_auth() -> None:
    advanced_auth = neo4j.kerberos_auth("foo bar")

    dci = DbmsConnectionInfo(uri="foo.bar", database="neo4j", auth=advanced_auth)

    assert dci.get_auth() == advanced_auth


def test_dbms_connection_info_aura_instance() -> None:
    dci = DbmsConnectionInfo(
        aura_instance_id="instance-id",
        username="neo4j",
        password="password",
        database="neo4j",
    )

    with pytest.raises(ValueError, match="'uri' is not provided."):
        dci.get_uri()

    dci.set_uri("neo4j+s://instance-id.databases.neo4j.io")

    assert dci.get_uri() == "neo4j+s://instance-id.databases.neo4j.io"


def test_dbms_connection_info_fail_on_auth_and_username() -> None:
    try:
        DbmsConnectionInfo(
            uri="foo.bar",
            username="neo4j",
            password="password",
            auth=neo4j.basic_auth("other", "other"),
        )
    except ValueError as e:
        assert str(e) == (
            "Cannot provide both username/password and token for authentication. "
            "Please provide either a username/password or a token."
        )
    else:
        assert False, "Expected ValueError was not raised"


def test_dbms_connection_info_fail_on_missing_instance_uri() -> None:
    with pytest.raises(ValueError, match="Either 'uri' or 'aura_instance_id' must be provided."):
        DbmsConnectionInfo(
            username="neo4j",
            password="password",
        )
