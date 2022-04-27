from typing import Any


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--include-enterprise", action="store_true", help="include tests requring GDS enterprise")
    parser.addoption(
        "--include-model-store-location",
        action="store_true",
        help="include tests for stored models",
    )
