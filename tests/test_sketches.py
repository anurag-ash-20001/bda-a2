"""Unit tests for the hand-written sketches:  python -m pytest tests  (or python tests/test_sketches.py)"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.sketches import DGIM, BloomFilter, MinHasher, Reservoir, jaccard, lsh_candidates  # noqa: E402


def test_dgim_error_bound():
    rng = random.Random(7)
    N = 10000
    d = DGIM(N)
    bits = []
    for i in range(50000):
        b = 1 if rng.random() < 0.12 else 0
        d.add(b)
        bits.append(b)
        if i % 997 == 0 and i > 0:
            for k in (1000, 5000, 10000):
                exact = sum(bits[-k:])
                est = d.query(k)
                if exact:
                    assert abs(est - exact) / exact <= 0.5, (i, k, exact, est)
    # at most two buckets per size, sizes non-decreasing going back in time
    sizes = [s for _, s in d.buckets]
    assert sizes == sorted(sizes)
    assert all(sizes.count(s) <= 2 for s in set(sizes))


def test_dgim_all_ones_and_zeros():
    d = DGIM(100)
    for _ in range(1000):
        d.add(0)
    assert d.query(100) == 0
    for _ in range(100):
        d.add(1)
    assert 50 <= d.query(100) <= 100


def test_bloom_no_false_negatives_and_fp_rate():
    bf = BloomFilter(10000, 0.01)
    for i in range(10000):
        bf.test_and_add(f"id-{i}")
    assert all(f"id-{i}" in bf for i in range(10000))
    fp = sum(f"other-{i}" in bf for i in range(20000)) / 20000
    assert fp < 0.02


def test_reservoir_mean():
    r = Reservoir(1000, 1, seed=1)
    for i in range(100000):
        r.add([i % 100])
    assert abs(r.mean(0) - 49.5) < 3


def test_lsh_finds_identical_sets():
    mh = MinHasher(100, seed=3)
    sets = {1: {1, 2, 3, 4, 5}, 2: {1, 2, 3, 4, 5}, 3: {100, 200, 300}}
    sigs = {k: mh.signature(v) for k, v in sets.items()}
    cands = lsh_candidates(sigs, 20, 5)
    assert (1, 2) in cands and (1, 3) not in cands
    assert jaccard(sets[1], sets[2]) == 1.0


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
