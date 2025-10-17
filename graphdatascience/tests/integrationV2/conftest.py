import os
from pathlib import Path
from typing import Any, Generator

import pytest


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if not config.getoption("--include-integration-v2"):
        skip_v2 = pytest.mark.skip(reason="need --include-integration-v2 option to run")
        for item in items:
            # otherwise would also skip lots of other test
            if "integrationV2" in str(item.fspath):
                item.add_marker(skip_v2)

    if inside_ci():
        skip_ci = pytest.mark.skip(reason="Skipping db_integration tests in CI")
        for item in items:
            if "db_integration" in item.keywords:
                item.add_marker(skip_ci)


# best used with pytest --basetemp=tmp/pytest for easy access to logs
@pytest.fixture(scope="session")
def logs_dir(tmp_path_factory: pytest.TempPathFactory) -> Generator[Path, None, None]:
    """Create a temporary file and return its path."""
    tmp_dir = tmp_path_factory.mktemp("logs")

    yield tmp_dir


def inside_ci() -> bool:
    return os.environ.get("BUILD_NUMBER") is not None
