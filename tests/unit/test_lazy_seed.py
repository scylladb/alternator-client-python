# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare lazy state reconstruction with the original eager sequence."""

from collections.abc import Iterator

import pytest

from alternator.core.deterministic_rand import DeterministicRand, _rng_cooked
from alternator.core.query_plan import LazyQueryPlan

_SEEDS = [
    0,
    1,
    -1,
    42,
    12345,
    (1 << 31) - 1,
    -(1 << 31),
    (1 << 63) - 1,
    -(1 << 63),
    1 << 256,
    -(1 << 256),
]


def _seed_step(value: int) -> int:
    """Original Schrage recurrence, independent of the jump-ahead factors."""
    value = 48271 * (value % 44488) - 3399 * (value // 44488)
    return value + 2147483647 if value < 0 else value


def _eager_values(seed: int) -> Iterator[int]:
    """Original eager initialization and lagged-Fibonacci update."""
    mask = (1 << 64) - 1
    state = []
    value = seed % 2147483647 or 89482311
    for index in range(-20, 607):
        value = _seed_step(value)
        if index >= 0:
            slot = value << 40
            value = _seed_step(value)
            slot ^= value << 20
            value = _seed_step(value)
            state.append((slot ^ value ^ _rng_cooked[index]) & mask)
    tap, feed = 0, 334
    while True:
        tap = (tap - 1) % 607
        feed = (feed - 1) % 607
        state[feed] = (state[feed] + state[tap]) & mask
        yield state[feed] & ((1 << 63) - 1)


def _reference_intn(values: Iterator[int], bound: int) -> int:
    """Original rejection sampling, including its 31-bit fast path."""
    bits = 31 if bound <= (1 << 31) - 1 else 63
    value = next(values) >> (63 - bits)
    if bound & (bound - 1) == 0:
        return value & (bound - 1)
    maximum = (1 << bits) - 1 - (1 << bits) % bound
    while value > maximum:
        value = next(values) >> (63 - bits)
    return value % bound


@pytest.mark.parametrize("seed", _SEEDS)
def test_lazy_sequence_matches_eager_through_state_wraps(seed: int) -> None:
    """Exercise sparse slots, updated slots, wraparound, and rejection paths."""
    expected = _eager_values(seed)
    actual = DeterministicRand(seed)
    bounds = [
        1,
        2,
        3,
        6,
        10,
        100,
        (1 << 30) + 1,
        (1 << 31) + 1,
        (1 << 62) + 1,
        (1 << 63) - 1,
    ]
    for index in range(2100):
        if index % 3 == 0:
            assert actual.int63() == next(expected)
        else:
            bound = bounds[index % len(bounds)]
            assert actual.intn(bound) == _reference_intn(expected, bound)
    assert len(actual._vec) <= 607

    # A reused generator must forget both reconstructed and updated slots.
    actual.seed(-seed)
    expected = _eager_values(-seed)
    assert not actual._vec
    assert [actual.int63() for _ in range(20)] == [next(expected) for _ in range(20)]


def test_first_draw_initializes_only_two_slots() -> None:
    rng = DeterministicRand(42)
    assert not rng._vec
    rng.int63()
    assert set(rng._vec) == {333, 606}


def test_zero_is_a_valid_cached_state_slot() -> None:
    rng = DeterministicRand(42)
    rng._vec[333] = 0
    rng._vec[606] = 0
    assert rng.int63() == 0


@pytest.mark.parametrize("seed", [0, -1, 42, 1 << 256])
@pytest.mark.parametrize("count", [0, 1, 3, 10, 100, 1024])
def test_complete_retry_orders_match_eager(seed: int, count: int) -> None:
    nodes = [f"node-{index}" for index in range(count)]
    remaining = nodes.copy()
    expected = []
    values = _eager_values(seed)
    while remaining:
        pick = _reference_intn(values, len(remaining))
        expected.append(remaining[pick])
        last = remaining.pop()
        if pick < len(remaining):
            remaining[pick] = last
    assert list(LazyQueryPlan(nodes, seed)) == expected
