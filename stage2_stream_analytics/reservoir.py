"""Reservoir sampling (Algorithm R) used by Stage 2 for the mean fare / trip distance."""
import numpy as np


class Reservoir:
    """Algorithm R: uniform sample of k items from a stream of unknown length."""

    def __init__(self, k, n_fields, seed=0):
        self.k = int(k)
        self.items = np.zeros((self.k, n_fields), dtype=np.float64)
        self.n = 0
        self.rng = np.random.default_rng(seed)

    def add(self, row):
        if self.n < self.k:
            self.items[self.n] = row
        else:
            j = self.rng.integers(0, self.n + 1)
            if j < self.k:
                self.items[j] = row
        self.n += 1

    def mean(self, field):
        size = min(self.n, self.k)
        return float(self.items[:size, field].mean()) if size else float("nan")
