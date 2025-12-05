import pytest

from graphdatascience.call_parameters import CallParameters


def test_key_order() -> None:
    params = CallParameters()

    params["b"] = 1
    params["a"] = 3

    assert params.placeholder_str() == "$b, $a"


def test_empty_params() -> None:
    params = CallParameters()

    assert params.placeholder_str() == ""


def test_get_job_id() -> None:
    # empty params
    params = CallParameters({})
    job_id = params.get_job_id()
    assert job_id is None

    # empty job id
    params = CallParameters(config={job_id: None})
    job_id = params.get_job_id()
    assert job_id is None

    # job_id given
    params = CallParameters(config={"job_id": "foo"})
    job_id = params.get_job_id()
    assert job_id == "foo"

    # jobId given
    params = CallParameters(config={"jobId": "bar"})
    job_id = params.get_job_id()
    assert job_id == "bar"


def test_ensure_job_id() -> None:
    # empty params
    params = CallParameters({})
    with pytest.raises(AssertionError, match="config is not set"):
        params.ensure_job_id_in_config()

    # empty job id
    params = CallParameters(config={"job_id": None})
    job_id = params.ensure_job_id_in_config()
    assert job_id is not None and job_id != ""

    # job_id given
    params = CallParameters(config={"job_id": "foo"})
    job_id = params.ensure_job_id_in_config()
    assert job_id == "foo"

    # jobId given
    params = CallParameters(config={"jobId": "bar"})
    job_id = params.ensure_job_id_in_config()
    assert job_id == "bar"
