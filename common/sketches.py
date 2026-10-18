"""Streaming algorithms used by the pipeline.

Bloom filter, DGIM, reservoir sampling and MinHash/LSH banding are implemented
here so that every parameter is visible and reportable. (HyperLogLog comes from
Spark's built-in HLL++ in Stage 2.)
"""
import math
from collections import defaultdict
from itertools import combinations

import mmh3
import numpy as np


# --------------------------------------------------------------------------- Bloom
class BloomFilter:
    """Standard Bloom filter sized from (expected items n, target false-positive rate p).

        m = -n ln p / (ln 2)^2      bits
        k = (m / n) ln 2            hash functions

    The k bit positions come from double hashing of one 128-bit MurmurHash3:
        g_i(x) = h1(x) + i * h2(x)  (mod m)
    """

    def __init__(self, expected_items, fp_rate):
        self.n_expected = int(expected_items)
        self.target_fp = float(fp_rate)
        self.m = max(8, int(math.ceil(-self.n_expected * math.log(self.target_fp) / math.log(2) ** 2)))
        self.k = max(1, int(round(self.m / self.n_expected * math.log(2))))
        self.bits = bytearray((self.m + 7) // 8)
        self.inserted = 0

    def _positions(self, key):
        h1, h2 = mmh3.hash64(key, signed=False)
        return [(h1 + i * h2) % self.m for i in range(self.k)]

    def test_and_add(self, key):
        """Return True if key was (probably) seen before, then insert it."""
        seen = True
        for pos in self._positions(key):
            byte, bit = divmod(pos, 8)
            mask = 1 << bit
            if not self.bits[byte] & mask:
                seen = False
                self.bits[byte] |= mask
        if not seen:
            self.inserted += 1
        return seen

    def __contains__(self, key):
        for pos in self._positions(key):
            byte, bit = divmod(pos, 8)
            if not self.bits[byte] & (1 << bit):
                return False
        return True

    def expected_fp_rate(self):
        """Theoretical FP probability after `inserted` distinct insertions."""
        return (1 - math.exp(-self.k * self.inserted / self.m)) ** self.k

    def params(self):
        return {"expected_items": self.n_expected, "target_fp": self.target_fp,
                "bit_array_size": self.m, "num_hashes": self.k, "inserted_ids": self.inserted,
                "memory_bytes": len(self.bits),
                "theoretical_fp_now": round(self.expected_fp_rate(), 6)}


# --------------------------------------------------------------------------- DGIM
class DGIM:
    """Datar-Gionis-Indyk-Motwani counter of 1s in the last N bits of a binary stream.

    Each bucket is (timestamp of its most recent 1, size), size a power of two.
    Invariant: at most two buckets of each size; when a third appears the two
    oldest of that size merge into one of double size (keeping the newer
    timestamp). Buckets whose timestamp falls out of the window are dropped.
    Memory: O(log^2 N) bits; relative error of a query <= 50%.
    """

    def __init__(self, window):
        self.N = int(window)
        self.t = 0                 # number of bits seen so far
        self.buckets = []          # newest first: [timestamp, size]

    def add(self, bit):
        self.t += 1
        # expire the oldest bucket once its most recent 1 left the window
        if self.buckets and self.buckets[-1][0] <= self.t - self.N:
            self.buckets.pop()
        if not bit:
            return
        self.buckets.insert(0, [self.t, 1])
        size = 1
        i = 0
        while True:
            # buckets of `size` are contiguous; find them starting at index i
            j = i
            while j < len(self.buckets) and self.buckets[j][1] == size:
                j += 1
            if j - i <= 2:
                break
            # three buckets of this size at i, i+1, i+2: merge the two oldest (i+1, i+2)
            newer_ts = self.buckets[i + 1][0]
            self.buckets[i + 1:i + 3] = [[newer_ts, size * 2]]
            i += 1
            size *= 2

    def query(self, k=None):
        """Estimate number of 1s among the last k bits (k <= N)."""
        k = self.N if k is None else min(int(k), self.N)
        total = 0
        last_size = 0
        for ts, size in self.buckets:
            if ts <= self.t - k:
                break
            total += size
            last_size = size
        if last_size == 0:
            return 0
        # only the oldest bucket may straddle the window boundary: count half of it.
        # A size-1 bucket is exactly its own timestamp, so it is fully inside.
        return total - last_size + (1 if last_size == 1 else last_size / 2)

    def num_buckets(self):
        return len(self.buckets)


# --------------------------------------------------------------------------- Reservoir
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


# --------------------------------------------------------------------------- MinHash + LSH
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
