from typing import Any


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--include-enterprise", action="store_true", help="include tests requring GDS enterprise")
    parser.addoption(
        "--encrypted-only", action="store_true", help="runs only tests requiring an encrypted connection to Neo4j"
    )
    parser.addoption(
        "--include-model-store-location",
        action="store_true",
        help="include tests for stored models",
    )
