from graphdatascience.utils import as_list


def test_as_list_wraps_a_single_string() -> None:
    assert as_list("Page") == ["Page"]


def test_as_list_passes_lists_through() -> None:
    assert as_list(["Page", "Link"]) == ["Page", "Link"]


def test_as_list_keeps_none() -> None:
    assert as_list(None) is None
