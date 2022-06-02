from abc import ABC

import pandas
from pandas.core.series import Series

from .model import Model


class PipelineModel(Model, ABC):
    def best_parameters(self) -> Series:
        return pandas.Series(self._list_info()["modelInfo"][0]["bestParameters"])
