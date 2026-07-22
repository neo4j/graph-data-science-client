"""Random relationship (edge) generators (vendored, adapted).

By default generators produce **distinct** ``(source, target)`` pairs — no
duplicate relationships. Self-loops (``source == target``) are always allowed and
count as ordinary pairs.

Set ``allow_duplicates=True`` to permit repeated pairs (sampling with
replacement). When distinct pairs are required but ``rel_count`` exceeds the
number of possible distinct pairs (``source * target``), a :class:`ValueError`
is raised up front rather than silently returning duplicates; when duplicates are
allowed but unavoidable, a warning is emitted instead.
"""

import random
import warnings
from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class RelationshipGenerator(ABC):
    def __init__(self, allow_duplicates: bool = False):
        self.allow_duplicates = allow_duplicates

    @abstractmethod
    def generate_edges(
        self, source_node_count: int, target_node_count: int, rel_count: int
    ) -> tuple[pd.Series, pd.Series]:
        pass

    def check(self, source_node_count: int, target_node_count: int, rel_count: int) -> None:
        """Validate feasibility early: raise if impossible, warn if duplicates are unavoidable."""
        space = source_node_count * target_node_count
        if rel_count > 0 and space == 0:
            raise ValueError("Cannot generate relationships: source or target node count is 0.")
        if rel_count > space:
            if not self.allow_duplicates:
                raise ValueError(
                    f"Cannot generate {rel_count} distinct relationships: only {space} distinct "
                    f"(source, target) pairs are possible for {source_node_count} x {target_node_count} "
                    "nodes. Reduce rel_count, or set allow_duplicates=True to permit repeated relationships."
                )
            warnings.warn(
                f"{rel_count} relationships requested but only {space} distinct (source, target) pairs "
                f"exist for {source_node_count} x {target_node_count} nodes; the result will contain "
                "repeated relationships.",
                stacklevel=2,
            )


class UniformRelationshipGenerator(RelationshipGenerator):
    """(source, target) pairs sampled uniformly."""

    def generate_edges(
        self, source_node_count: int, target_node_count: int, rel_count: int
    ) -> tuple[pd.Series, pd.Series]:
        self.check(source_node_count, target_node_count, rel_count)

        if self.allow_duplicates:
            src = [random.randrange(source_node_count) for _ in range(rel_count)]
            tgt = [random.randrange(target_node_count) for _ in range(rel_count)]
            return pd.Series(src, dtype=int), pd.Series(tgt, dtype=int)

        # Distinct pairs: sample flat indices into the source x target grid.
        flat = random.sample(range(source_node_count * target_node_count), rel_count)
        src = [i // target_node_count for i in flat]
        tgt = [i % target_node_count for i in flat]
        return pd.Series(src, dtype=int), pd.Series(tgt, dtype=int)


class PowerLawRelationshipGenerator(RelationshipGenerator):
    def __init__(
        self,
        alpha: float = 0.9,
        rng: np.random.Generator | None = None,
        allow_duplicates: bool = False,
    ):
        super().__init__(allow_duplicates=allow_duplicates)
        self.alpha = alpha
        self.rng = rng if rng is not None else np.random.default_rng()

    def generate_edges(
        self, source_node_count: int, target_node_count: int, rel_count: int
    ) -> tuple[pd.Series, pd.Series]:
        self.check(source_node_count, target_node_count, rel_count)

        def sample(n: int, size: int) -> np.ndarray:
            if n <= 1:
                return np.zeros(size, dtype=int)
            x = self.rng.pareto(1 + self.alpha, size=size)
            return np.minimum((x / (1 + x) * n).astype(int), n - 1)  # type: ignore[no-any-return]

        if self.allow_duplicates:
            return (
                pd.Series(sample(source_node_count, rel_count), dtype=int),
                pd.Series(sample(target_node_count, rel_count), dtype=int),
            )

        # Distinct pairs: rejection sampling keeps the power-law hub structure.
        seen: set[tuple[int, int]] = set()
        src: list[int] = []
        tgt: list[int] = []
        for _ in range(1000):
            if len(seen) >= rel_count:
                break
            batch = max((rel_count - len(seen)) * 2, 16)
            s = sample(source_node_count, batch)
            t = sample(target_node_count, batch)
            for a, b in zip(s.tolist(), t.tolist()):
                if (a, b) not in seen:
                    seen.add((a, b))
                    src.append(a)
                    tgt.append(b)
                    if len(seen) >= rel_count:
                        break
        return pd.Series(src, dtype=int), pd.Series(tgt, dtype=int)
