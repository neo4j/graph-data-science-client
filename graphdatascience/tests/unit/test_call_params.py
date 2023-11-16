from graphdatascience.call_parameters import CallParameters


def test_key_order() -> None:
    params = CallParameters()

    params["b"] = 1
    params["a"] = 3

    assert params.placeholder_str() == "$b, $a"


def test_empty_params() -> None:
    params = CallParameters()

    assert params.placeholder_str() == ""
