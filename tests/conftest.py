from typing import Any


def pytest_addoption(parser: Any) -> None:
    parser.addoption("--include-ogb", action="store_true", help="include tests requiring the ogb dependency")
