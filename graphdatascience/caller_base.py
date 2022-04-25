from abc import ABC

from .query_runner.query_runner import QueryRunner
from .server_version.server_version import ServerVersion


class CallerBase(ABC):
    def __init__(self, query_runner: QueryRunner, namespace: str, server_version: ServerVersion):
        self._query_runner = query_runner
        self._namespace = namespace
        self._server_version = server_version
