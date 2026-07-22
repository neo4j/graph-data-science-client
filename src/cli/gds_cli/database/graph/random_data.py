"""Random property-column generators (vendored, unchanged)."""

import random
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class RandomGenerator(ABC):
    @abstractmethod
    def generate(self, length: int) -> pd.Series:
        pass


class GaussianGenerator(RandomGenerator):
    def __init__(self, mean: float = 0.0, std: float = 1.0, dim: int = 0, rng: np.random.Generator | None = None):
        self.mean = mean
        self.std = std
        self.dim = dim
        self._rng = rng if rng is not None else np.random.default_rng()

    def generate(self, length: int) -> pd.Series:
        if self.dim == 0:
            return pd.Series(self.mean + self._rng.standard_normal(size=length) * self.std)
        # dim > 0 -> one float vector per row. tolist() yields native Python floats
        # so the Neo4j driver stores them as an array property (numpy arrays don't serialize).
        values = self.mean + self._rng.standard_normal(size=(length, self.dim)) * self.std
        return pd.Series(values.tolist())


class UniformIntegerGenerator(RandomGenerator):
    def __init__(self, low: int, high: int, dim: int = 0):
        self.low = low
        self.high = high
        self.dim = dim

    def generate(self, length: int) -> pd.Series:
        if self.dim == 0:
            return pd.Series(np.random.randint(self.low, self.high, size=length))
        # dim > 0 -> one int vector per row, as native Python ints (see GaussianGenerator).
        return pd.Series(np.random.randint(self.low, self.high, size=(length, self.dim)).tolist())


class UniformTimestampGenerator(RandomGenerator):
    def __init__(self, start: pd.Timestamp, end: pd.Timestamp):
        self.start = start
        self.end = end

    def generate(self, length: int) -> pd.Series:
        return pd.Series(np.random.randint(self.start.value, self.end.value, size=length))


class UniformStringCategoryGenerator(RandomGenerator):
    def __init__(self, categories: list[str]):
        self.categories = categories

    def generate(self, length: int) -> pd.Series:
        return pd.Series([random.choice(self.categories) for _ in range(length)])
