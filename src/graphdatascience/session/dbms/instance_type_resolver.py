from neo4j.exceptions import Neo4jError

from graphdatascience.query_runner import QueryMode, QueryRunner, QueryType
from graphdatascience.session.instance_type import InstanceType


class InstanceTypeResolver:
    """Resolves the type of the database a query runner is connected to."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner

    def resolve(self) -> InstanceType:
        try:
            result = self._query_runner.run_retryable_cypher(
                # The runner's database names the row; without one it connects to the user's home database.
                "SHOW DATABASES YIELD name, type, home WHERE name = $database OR (home AND $database IS NULL) RETURN type",
                QueryType.SYSTEM,
                params={"database": self._query_runner.database() or None},
                mode=QueryMode.READ,
            )
        except Neo4jError:
            # Not every user is allowed to list databases; keep the default behavior in that case.
            return InstanceType.DATABASE

        if result.empty:
            return InstanceType.DATABASE

        return InstanceType.from_show_databases_type(str(result.iloc[0]["type"]))
