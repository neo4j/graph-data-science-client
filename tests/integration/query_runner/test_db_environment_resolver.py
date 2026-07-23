from graphdatascience.graph_data_science import GraphDataScience
from graphdatascience.query_runner.db_environment_resolver import DbEnvironmentResolver
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner
from graphdatascience.session.dbms_connection_info import DbmsConnectionInfo


def test_hosted_in_aura_aura_dbms(query_runner: Neo4jQueryRunner) -> None:
    assert not DbEnvironmentResolver.hosted_in_aura(query_runner)


def test_derive_aura_ds_for_non_aura_dbms(neo4j_connection: DbmsConnectionInfo) -> None:
    # The local test database is not hosted in Aura, so the derived flag must be False.
    assert (
        GraphDataScience._derive_aura_ds(
            f"bolt://{neo4j_connection.uri}",
            neo4j_connection.get_auth(),
            database="neo4j",
        )
        is False
    )
