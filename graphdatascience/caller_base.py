from abc import ABC
from typing import NoReturn

from .error.endpoint_suggester import generate_suggestive_error_message
from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion


class CallerBase(ABC):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        self._query_runner = query_runner
        self._namespace = namespace
        self._server_version = server_version

    def _raise_suggestive_error_message(self, requested_endpoint: str) -> NoReturn:
        list_result = self._query_runner.call_procedure(
            endpoint="gds.list",
            yields=["name"],
            custom_error=False,
        )
        all_endpoints = list_result["name"].tolist()

        raise SyntaxError(generate_suggestive_error_message(requested_endpoint, all_endpoints))
