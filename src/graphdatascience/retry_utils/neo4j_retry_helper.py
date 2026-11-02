import neo4j

from graphdatascience.versions import SemanticVersion

# neo4j < 6.0 signals pool-acquisition timeouts as a plain ClientError with this
# message; 6.0 introduced a dedicated exception with the text unchanged
_POOL_ACQUISITION_TIMEOUT_PREFIX = "failed to obtain a connection from the pool within"
_NEO4J_DRIVER_VERSION = SemanticVersion.from_string(neo4j.__version__)


def _is_pool_acquisition_timeout(exception: BaseException) -> bool:
    if _NEO4J_DRIVER_VERSION < SemanticVersion(6, 0, 0):
        return isinstance(exception, neo4j.exceptions.ClientError) and str(exception).startswith(
            _POOL_ACQUISITION_TIMEOUT_PREFIX
        )
    return isinstance(exception, neo4j.exceptions.ConnectionAcquisitionTimeoutError)


def is_retryable_neo4j_exception(exception: BaseException) -> bool:
    # raised during connection acquisition, before anything is sent; the driver
    # marks it non-retryable because an exhausted pool is not necessarily
    # transient for query execution
    if _is_pool_acquisition_timeout(exception):
        return True
    if isinstance(exception, neo4j.exceptions.Neo4jError | neo4j.exceptions.DriverError) and hasattr(
        exception, "is_retryable"
    ):
        return exception.is_retryable()
    return False
