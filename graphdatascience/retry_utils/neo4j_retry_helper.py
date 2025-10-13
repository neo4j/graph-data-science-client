import neo4j


def is_retryable_neo4j_exception(exception: BaseException) -> bool:
    if neo4j.__version__.startswith("4."):
        return False

    if isinstance(exception, neo4j.exceptions.Neo4jError | neo4j.exceptions.DriverError) and hasattr(
        exception, "is_retryable"
    ):
        return exception.is_retryable()
    return False
