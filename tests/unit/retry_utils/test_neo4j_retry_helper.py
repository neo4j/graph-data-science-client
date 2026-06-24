import neo4j

from graphdatascience.retry_utils.neo4j_retry_helper import is_retryable_neo4j_exception


def test_is_retryable_neo4j_exception() -> None:
    assert is_retryable_neo4j_exception(neo4j.exceptions.TransientError("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.SessionExpired("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.ReadServiceUnavailable("test"))

    assert not is_retryable_neo4j_exception(neo4j.exceptions.AuthError("test"))
    assert not is_retryable_neo4j_exception(ValueError("test"))
    assert not is_retryable_neo4j_exception(RuntimeError("test"))
    assert not is_retryable_neo4j_exception(Exception("test"))
