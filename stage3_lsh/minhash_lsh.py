"""MinHash signatures, LSH banding and exact Jaccard used by Stage 3."""
from collections import defaultdict
from itertools import combinations

import numpy as np


MERSENNE_P = (1 << 31) - 1


class MinHasher:
    """MinHash with universal hash family h_i(x) = (a_i x + b_i) mod p, p = 2^31 - 1."""

    def __init__(self, num_hashes, seed=1):
        rng = np.random.default_rng(seed)
        self.a = rng.integers(1, MERSENNE_P, size=num_hashes, dtype=np.int64)
        self.b = rng.integers(0, MERSENNE_P, size=num_hashes, dtype=np.int64)

    def signature(self, elements):
        x = np.fromiter(elements, dtype=np.int64)
        return ((self.a[:, None] * x[None, :] + self.b[:, None]) % MERSENNE_P).min(axis=1)


def lsh_candidates(signatures, bands, rows):
    """Banding: items sharing an identical band (r consecutive signature values)
    in at least one band become a candidate pair. Returns set of (p, q) with p < q."""
    candidates = set()
    for b in range(bands):
        buckets = defaultdict(list)
        lo, hi = b * rows, (b + 1) * rows
        for key, sig in signatures.items():
            buckets[(b, sig[lo:hi].tobytes())].append(key)
        for members in buckets.values():
            if len(members) > 1:
                candidates.update(combinations(sorted(members), 2))
    return candidates


def jaccard(a, b):
    union = len(a | b)
    return len(a & b) / union if union else 0.0


def lsh_threshold(bands, rows):
    """Approximate similarity at which P(candidate) = 1/2: (1/b)^(1/r)."""
    return (1.0 / bands) ** (1.0 / rows)
