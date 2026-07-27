from graphdatascience.query_runner.query_runner import QueryRunner

# Fields of `GraphInfo`. Yielded explicitly to skip the deprecated `schema` column,
# which the server warns about whenever it is returned.
GRAPH_INFO_YIELDS = [
    "graphName",
    "database",
    "databaseLocation",
    "configuration",
    "memoryUsage",
    "sizeInBytes",
    "nodeCount",
    "relationshipCount",
    "creationTime",
    "modificationTime",
    "schemaWithOrientation",
    "density",
]

# Fields of `GraphInfoWithDegrees`.
GRAPH_INFO_WITH_DEGREES_YIELDS = GRAPH_INFO_YIELDS + ["degreeDistribution"]


def require_database(query_runner: QueryRunner) -> str:
    database = query_runner.database()
    if database is None:
        raise ValueError(
            "For this call you must have explicitly specified a valid Neo4j database to target, "
            "using `gds.set_database`."
        )

    return database
