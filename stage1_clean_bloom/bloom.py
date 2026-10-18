"""Bloom filter used by Stage 1 for duplicate membership on event_id (own implementation)."""
import math

import mmh3


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
