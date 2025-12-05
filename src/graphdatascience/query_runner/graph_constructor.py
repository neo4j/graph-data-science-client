from abc import ABC, abstractmethod

from pandas import DataFrame


class GraphConstructor(ABC):
    @abstractmethod
    def run(self, node_dfs: list[DataFrame], relationship_dfs: list[DataFrame]) -> None:
        pass
