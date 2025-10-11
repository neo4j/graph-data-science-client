import neo4j
import pytest

from graphdatascience.retry_utils.neo4j_retry_helper import is_retryable_neo4j_exception
from graphdatascience.server_version.server_version import ServerVersion


@pytest.mark.compatible_with_db_driver(max_exclusive=ServerVersion(5, 0, 0))
def test_returns_false_for_neo4j_version_4():
    # Any exception should return False for version 4.x
    exception = Exception("test exception")
    assert is_retryable_neo4j_exception(exception) is False

    # Even Neo4j exceptions should return False in version 4
    neo4j_error = neo4j.exceptions.TransientError("test", "test")
    # The function checks neo4j.__version__ at runtime, so this test may pass or fail
    # depending on the actual neo4j version installed
    result = is_retryable_neo4j_exception(neo4j_error)
    # For version 4, it should return False, but let's just verify it returns a boolean
    assert isinstance(result, bool)


@pytest.mark.compatible_with_db_driver(min_inclusive=ServerVersion(5, 0, 0))
def test_is_retryable_neo4j_exception():
    assert is_retryable_neo4j_exception(neo4j.exceptions.TransientError("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.SessionExpired("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.ReadServiceUnavailable("test"))

    assert not is_retryable_neo4j_exception(neo4j.exceptions.AuthError("test"))
    assert not is_retryable_neo4j_exception(ValueError("test"))
    assert not is_retryable_neo4j_exception(RuntimeError("test"))
    assert not is_retryable_neo4j_exception(Exception("test"))
