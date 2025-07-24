from typing import Any

import pytest


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    if not config.getoption("--include-integration-v2"):
        skip_v2 = pytest.mark.skip(reason="need --include-integration-v2 option to run")
        for item in items:
            item.add_marker(skip_v2)
