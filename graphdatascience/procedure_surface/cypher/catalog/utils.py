from graphdatascience.query_runner.query_runner import QueryRunner


def require_database(query_runner: QueryRunner) -> str:
    database = query_runner.database()
    if database is None:
        raise ValueError(
            "For this call you must have explicitly specified a valid Neo4j database to target, "
            "using `gds.set_database`."
        )

    return database
