from __future__ import annotations

import math

Number = float | int


class SimilarityFunctions:
    def jaccard(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the Jaccard similarity between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Jaccard similarity in [0, 1].
        """

        a = sorted(vector1)
        b = sorted(vector2)

        index1 = 0
        index2 = 0

        intersection = 0.0
        union = 0.0

        while index1 < len(a) and index2 < len(b):
            val1 = a[index1]
            val2 = b[index2]

            if val1 == val2:
                intersection += 1
                union += 1
                index1 += 1
                index2 += 1
            elif val1 < val2:
                union += 1
                index1 += 1
            else:
                union += 1
                index2 += 1

        union += len(a) - index1 + len(b) - index2

        if union == 0:
            return 0.0

        return intersection / union

    def overlap(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the overlap coefficient between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Overlap coefficient in [0, 1].
        """
        self._check_vector_lengths(vector1, vector2)

        set1 = {v for v in vector1}
        set2 = {v for v in vector2}

        min_size = min(len(vector1), len(vector2))
        if min_size == 0:
            return 0.0

        return len(set1 & set2) / min_size

    def cosine(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the cosine similarity between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Cosine similarity in [-1, 1].
        """
        self._check_vector_lengths(vector1, vector2)

        dot_product = sum(a * b for a, b in zip(vector1, vector2))
        magnitude1 = math.sqrt(sum(a**2 for a in vector1))
        magnitude2 = math.sqrt(sum(b**2 for b in vector2))

        if magnitude1 == 0.0 or magnitude2 == 0.0:
            return 0.0

        return dot_product / (magnitude1 * magnitude2)

    def pearson(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the Pearson correlation coefficient between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Pearson correlation in [-1, 1].
        """
        self._check_vector_lengths(vector1, vector2)

        n = len(vector1)
        if n == 0:
            return 0.0

        mean1 = sum(vector1) / n
        mean2 = sum(vector2) / n

        numerator = sum((a - mean1) * (b - mean2) for a, b in zip(vector1, vector2))
        denom1 = math.sqrt(sum((a - mean1) ** 2 for a in vector1))
        denom2 = math.sqrt(sum((b - mean2) ** 2 for b in vector2))

        if denom1 == 0.0 or denom2 == 0.0:
            return 0.0

        return numerator / (denom1 * denom2)

    def euclidean_distance(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the Euclidean distance between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Euclidean distance in [0, inf).
        """
        self._check_vector_lengths(vector1, vector2)

        return math.sqrt(sum((a - b) ** 2 for a, b in zip(vector1, vector2)))

    def euclidean(self, vector1: list[Number], vector2: list[Number]) -> float:
        """
        Compute the normalized Euclidean similarity between two vectors.

        Parameters
        ----------
        vector1
            First input vector.
        vector2
            Second input vector.

        Returns
        -------
        float
            Euclidean similarity in (0, 1].
        """
        return 1.0 / (1.0 + self.euclidean_distance(vector1, vector2))

    def _check_vector_lengths(self, vector1: list[Number], vector2: list[Number]) -> None:
        if len(vector1) != len(vector2):
            raise ValueError(f"Vectors must have the same length, got {len(vector1)} and {len(vector2)}")
