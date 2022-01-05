from gdsclient.graph_data_science import GraphDataScience
from gdsclient.query_runner.neo4j_query_runner import Neo4jQueryRunner

PIPE_NAME = "pipe"


def test_create_lp_pipeline(runner: Neo4jQueryRunner, gds: GraphDataScience) -> None:
    pipe = gds.alpha.ml.pipeline.linkPrediction.create(PIPE_NAME)
    assert pipe.name() == PIPE_NAME

    query = "CALL gds.beta.model.exists($name)"
    params = {"name": pipe.name()}
    result = runner.run_query(query, params)
    assert result[0]["exists"]

    query = "CALL gds.beta.model.drop($name)"
    params = {"name": pipe.name()}
    runner.run_query(query, params)
