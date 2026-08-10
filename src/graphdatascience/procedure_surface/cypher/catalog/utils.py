from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.query_mode import QueryMode
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


def drop_graph_if_exists(cypher_runner: QueryRunner, graph_name: str, username: str | None = None) -> None:
    """Idempotently drop a graph, ignoring it if it does not exist.

    When ``username`` is given, the drop is performed as that (impersonated) user so that a
    graph owned by them is removed — matching the user context of the subsequent create call.
    """
    params = CallParameters(graphName=graph_name, failIfMissing=False)

    if username is not None:
        # positional params. order has to be preserved
        params["dbName"] = ""
        params["username"] = username

    cypher_runner.call_procedure(
        endpoint="gds.graph.drop",
        params=params,
        yields=GRAPH_INFO_YIELDS,
        # dropping is idempotent as long as a missing graph is not an error
        retryable=True,
        mode=QueryMode.WRITE,
    )
