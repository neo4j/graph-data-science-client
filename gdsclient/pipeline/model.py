from abc import ABC
from typing import Any, Dict

from ..query_runner.query_runner import QueryRunner


class Model(ABC):
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def _list_info(self) -> Dict[str, Any]:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}

        info = self._query_runner.run_query(query, params)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the model catalog")

        return info[0]

    def name(self) -> str:
        return self._name

    def type(self) -> str:
        return self._list_info()["modelInfo"]["modelType"]  # type: ignore

    def train_config(self) -> Dict[str, Any]:
        return self._list_info()["trainConfig"]  # type: ignore

    def graph_schema(self) -> Dict[str, Any]:
        return self._list_info()["graphSchema"]  # type: ignore

    def loaded(self) -> bool:
        return self._list_info()["loaded"]  # type: ignore

    def stored(self) -> bool:
        return self._list_info()["stored"]  # type: ignore

    def creation_time(self) -> Any:  # neo4j.time.DateTime not exported
        return self._list_info()["creationTime"]

    def shared(self) -> bool:
        return self._list_info()["shared"]  # type: ignore
