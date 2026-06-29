import neo4j


def is_retryable_neo4j_exception(exception: BaseException) -> bool:
    if isinstance(exception, neo4j.exceptions.Neo4jError | neo4j.exceptions.DriverError) and hasattr(
        exception, "is_retryable"
    ):
        return exception.is_retryable()
    return False
