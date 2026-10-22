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

"""Tests for reusable backoff policies."""

import pytest

from alternator import Backoff, ExponentialBackoff


def test_exponential_default_sequence_and_cap() -> None:
    policy: Backoff = ExponentialBackoff()
    assert [policy.delay_seconds(attempt=attempt) for attempt in range(1, 10)] == [
        1,
        2,
        4,
        8,
        16,
        32,
        60,
        60,
        60,
    ]
    assert policy.delay_seconds(attempt=1) == 1  # No state is retained.


def test_custom_bounds_and_extreme_attempts() -> None:
    policy = ExponentialBackoff(initial_seconds=0.25, max_seconds=3)
    assert [policy.delay_seconds(attempt) for attempt in range(1, 6)] == [
        0.25,
        0.5,
        1,
        2,
        3,
    ]
    assert policy.delay_seconds(10**100) == 3
    assert ExponentialBackoff(5e-324, 1e308).delay_seconds(2099) == 1e308
    assert ExponentialBackoff(2, 2).delay_seconds(1) == 2


@pytest.mark.parametrize("initial", [0, -1, float("nan"), float("inf")])
def test_invalid_initial_delay(initial: float) -> None:
    with pytest.raises(ValueError, match="initial_seconds"):
        ExponentialBackoff(initial_seconds=initial)


@pytest.mark.parametrize("maximum", [0, -1, 0.5, float("nan"), float("inf")])
def test_invalid_maximum_delay(maximum: float) -> None:
    with pytest.raises(ValueError, match="max_seconds"):
        ExponentialBackoff(max_seconds=maximum)


@pytest.mark.parametrize("attempt", [0, -1, True, 1.5])
def test_invalid_attempt(attempt: int) -> None:
    with pytest.raises(ValueError, match="attempt"):
        ExponentialBackoff().delay_seconds(attempt)
