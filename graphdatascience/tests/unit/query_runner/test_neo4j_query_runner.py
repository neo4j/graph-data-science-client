from graphdatascience.call_parameters import CallParameters
from graphdatascience.query_runner.neo4j_query_runner import Neo4jQueryRunner


def test_job_id_extraction() -> None:
    # empty params
    params = CallParameters({})
    job_id = Neo4jQueryRunner._extract_or_create_job_id(params)
    assert job_id is not None and job_id != ""

    # empty job id
    params = CallParameters(config={job_id: None})
    job_id = Neo4jQueryRunner._extract_or_create_job_id(params)
    assert job_id is not None and job_id != ""

    # job_id given
    params = CallParameters(config={"job_id": "foo"})
    job_id = Neo4jQueryRunner._extract_or_create_job_id(params)
    assert job_id == "foo"

    # jobId given
    params = CallParameters(config={"jobId": "bar"})
    job_id = Neo4jQueryRunner._extract_or_create_job_id(params)
    assert job_id == "bar"
