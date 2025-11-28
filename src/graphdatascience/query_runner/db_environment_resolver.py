from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


class DbEnvironmentResolver:
    @staticmethod
    def hosted_in_aura(db_runner: Neo4jQueryRunner) -> bool:
        return (
            db_runner.run_retryable_cypher("""
        CALL dbms.components() YIELD name, versions
        WHERE name = "Neo4j Kernel"
        UNWIND versions as v
        WITH name, v
        WHERE v ENDS WITH "aura"
        RETURN count(*) <> 0
        """).squeeze()
            is True
        )
