import neo4j
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo


def test_dbms_connection_info_username_password() -> None:
    dci = DbmsConnectionInfo(
        uri="foo.bar",
        username="neo4j",
        password="password",
        database="neo4j",
    )

    assert dci.get_auth() == neo4j.basic_auth("neo4j", "password")


def test_dbms_connection_info_advanced_auth() -> None:
    advanced_auth = neo4j.kerberos_auth("foo bar")

    dci = DbmsConnectionInfo(uri="foo.bar", database="neo4j", auth=advanced_auth)

    assert dci.get_auth() == advanced_auth


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
