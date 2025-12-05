from typing import Any


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--include-enterprise", action="store_true", help="include tests requiring GDS enterprise")
    parser.addoption(
        "--encrypted-only", action="store_true", help="runs only tests requiring an encrypted connection to Neo4j"
    )
    parser.addoption(
        "--include-model-store-location",
        action="store_true",
        help="include tests for stored models",
    )
    parser.addoption("--target-aura", action="store_true", help="the database targeted is an AuraDS instance")
    parser.addoption("--include-ogb", action="store_true", help="include tests requiring the ogb dependency")
    parser.addoption(
        "--include-cloud-architecture", action="store_true", help="include tests resuiring a cloud architecture setup"
    )
    parser.addoption("--include-integration-v2", action="store_true", help="include integration tests for v2")
