import logging

from neo4j.exceptions import Neo4jError

from graphdatascience.query_runner import QueryMode, QueryRunner, QueryType
from graphdatascience.session.instance_type import InstanceType


class InstanceTypeResolver:
    """Resolves the type of the database a query runner is connected to."""

    def __init__(self, query_runner: QueryRunner):
        self._query_runner = query_runner
        self._logger = logging.getLogger(__name__)

    def resolve(self) -> InstanceType:
        try:
            result = self._query_runner.run_retryable_cypher(
                # The runner's database names the row; without one it connects to the user's home database.
                "SHOW DATABASES YIELD name, type, home WHERE name = $database OR (home AND $database IS NULL) RETURN type",
                QueryType.SYSTEM,
                params={"database": self._query_runner.database() or None},
                mode=QueryMode.READ,
            )
        except Neo4jError as e:
            # Not every user is allowed to list databases; keep the default behavior in that case.
            self._logger.warning(
                f"Could not determine the instance type of the connected database: {e}. Assuming a database."
            )
            return InstanceType.DATABASE

        if result.empty:
            self._logger.warning(
                "Could not determine the instance type: the connected database is not listed by `SHOW DATABASES`. "
                "Assuming a database."
            )
            return InstanceType.DATABASE

        return InstanceType.from_show_databases_type(str(result.iloc[0]["type"]))
