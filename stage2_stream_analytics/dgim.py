"""DGIM counter of recent 1s (slow_trip_bit) used by Stage 2 - implemented by the group."""


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
