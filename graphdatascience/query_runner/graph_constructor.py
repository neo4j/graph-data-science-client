from abc import ABC, abstractmethod
from typing import List

from pandas import DataFrame


class GraphConstructor(ABC):
    @abstractmethod
    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        pass
