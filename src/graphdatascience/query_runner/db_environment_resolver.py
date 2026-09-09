from graphdatascience.query_runner.query_runner import QueryRunner
from graphdatascience.query_runner.query_type import QueryType


class DbEnvironmentResolver:
    @staticmethod
    def hosted_in_aura(db_runner: QueryRunner) -> bool:
        return (
            db_runner.run_retryable_cypher(
                """
        CALL dbms.components() YIELD name, versions
        WHERE name = "Neo4j Kernel"
        UNWIND versions as v
        WITH name, v
        WHERE v ENDS WITH "aura"
        RETURN count(*) <> 0
        """,
                QueryType.SYSTEM,
            )
            .iloc[0]
            .item()
        ) is True
