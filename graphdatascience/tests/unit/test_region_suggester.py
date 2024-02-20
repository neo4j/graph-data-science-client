from graphdatascience.gds_session import region_suggester


def test_picks_the_only_option() -> None:
    match = region_suggester.closest_match("db_region", ["ds_region1"])
    assert match == "ds_region1"


def test_picks_the_closest() -> None:
    match = region_suggester.closest_match("eu-1", ["us-1", "aa-1", "ea-1", "eu-4"])
    assert match == "eu-4"


def test_picks_exact_match() -> None:
    match = region_suggester.closest_match("eu-2", ["eu-1", "eu-2", "eu-3"])
    assert match == "eu-2"


def test_no_options() -> None:
    match = region_suggester.closest_match("eu-2", [])
    assert match is None
