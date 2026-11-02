import neo4j

from graphdatascience.retry_utils.neo4j_retry_helper import is_retryable_neo4j_exception


def test_is_retryable_neo4j_exception() -> None:
    assert is_retryable_neo4j_exception(neo4j.exceptions.TransientError("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.SessionExpired("test"))
    assert is_retryable_neo4j_exception(neo4j.exceptions.ReadServiceUnavailable("test"))
    assert is_retryable_neo4j_exception(_pool_acquisition_timeout_error())

    assert not is_retryable_neo4j_exception(neo4j.exceptions.AuthError("test"))
    assert not is_retryable_neo4j_exception(ValueError("test"))
    assert not is_retryable_neo4j_exception(RuntimeError("test"))
    assert not is_retryable_neo4j_exception(Exception("test"))
    assert not is_retryable_neo4j_exception(neo4j.exceptions.ClientError("some other client error"))


def _pool_acquisition_timeout_error() -> Exception:
    # neo4j 6+ raises a dedicated class; earlier versions a plain ClientError
    # carrying the same message
    message = "failed to obtain a connection from the pool within 10.0s (timeout)"
    error_class: type[Exception] | None = getattr(neo4j.exceptions, "ConnectionAcquisitionTimeoutError", None)
    if error_class is not None:
        return error_class(message)
    return neo4j.exceptions.ClientError(message)
