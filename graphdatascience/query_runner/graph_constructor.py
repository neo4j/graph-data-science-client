from abc import ABC, abstractmethod
from typing import List

from pandas.core.frame import DataFrame


class GraphConstructor(ABC):
    @abstractmethod
    def run(self, node_dfs: List[DataFrame], relationship_dfs: List[DataFrame]) -> None:
        pass
