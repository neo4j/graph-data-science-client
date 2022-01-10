import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--include-enterprise", action="store_true", help="include enterprise tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--include-enterprise"):
        # --include-enterprise given in cli: do not skip enterprise tests
        return
    skip_enterprise = pytest.mark.skip(reason="need --include-enterprise option to run")
    for item in items:
        if "enterprise" in item.keywords:
            item.add_marker(skip_enterprise)
