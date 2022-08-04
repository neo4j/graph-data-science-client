from typing import Any


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--include-enterprise", action="store_true", help="include tests requring GDS enterprise")
    parser.addoption(
        "--enterprise-ssl", action="store_true", help="runs only tests requring GDS enterprise with SSL encryption"
    )
    parser.addoption(
        "--include-model-store-location",
        action="store_true",
        help="include tests for stored models",
    )
