from abc import ABC, abstractmethod
from typing import Any, Dict

from ..graph.graph_object import Graph
from ..query_runner.query_runner import QueryResult, QueryRunner, Row


class Model(ABC):
    def __init__(self, name: str, query_runner: QueryRunner):
        self._name = name
        self._query_runner = query_runner

    def _list_info(self) -> Row:
        query = "CALL gds.beta.model.list($name)"
        params = {"name": self.name()}

        info = self._query_runner.run_query(query, params)

        if len(info) == 0:
            raise ValueError(f"There is no '{self.name()}' in the model catalog")

        return info[0]

    def _estimate_predict(
        self, predict_mode: str, graph_name: str, config: Dict[str, Any]
    ) -> Row:
        query = f"{self._query_prefix()}{predict_mode}.estimate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": graph_name, "config": config}

        return self._query_runner.run_query(query, params)[0]

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

    def exists(self) -> bool:
        query = "CALL gds.beta.model.exists($model_name) YIELD exists"
        params = {"model_name": self._name}

        return self._query_runner.run_query(query, params)[0]["exists"]  # type: ignore

    def drop(self) -> None:
        query = "CALL gds.beta.model.drop($model_name)"
        params = {"model_name": self._name}

        self._query_runner.run_query(query, params)

    @abstractmethod
    def _query_prefix(self) -> str:
        pass

    def metrics(self) -> Dict[str, Any]:
        return self._list_info()["modelInfo"]["metrics"]  # type: ignore

    def predict_stream(self, G: Graph, **config: Any) -> QueryResult:
        query = f"{self._query_prefix()}stream($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)

    def predict_stream_estimate(self, G: Graph, **config: Any) -> Row:
        return self._estimate_predict("stream", G.name(), config)

    def predict_mutate(self, G: Graph, **config: Any) -> Row:
        query = f"{self._query_prefix()}mutate($graph_name, $config)"
        config["modelName"] = self.name()
        params = {"graph_name": G.name(), "config": config}

        return self._query_runner.run_query(query, params)[0]

    def predict_mutate_estimate(self, G: Graph, **config: Any) -> Row:
        return self._estimate_predict("mutate", G.name(), config)
