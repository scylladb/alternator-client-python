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

"""Reusable, stateless delay policies for repeated failures."""

from dataclasses import dataclass
from math import isfinite, ldexp
from typing import Protocol


class Backoff(Protocol):
    """A stateless delay policy, safe to share between tables and clients.

    Implementations must return a finite, non-negative delay without sleeping.
    The caller owns failure counts, clocks and scheduling.
    """

    def delay_seconds(self, attempt: int) -> float:
        """Return the delay after a failure; ``attempt=1`` is the first failure."""
        ...


@dataclass(frozen=True)
class ExponentialBackoff:
    """Double the delay after each failure, up to ``max_seconds``.

    Both bounds must be finite and positive, with the maximum at least the
    initial delay. Attempts start at one and reset after a successful operation.
    """

    initial_seconds: float = 1.0
    max_seconds: float = 60.0

    def __post_init__(self) -> None:
        if not isfinite(self.initial_seconds) or self.initial_seconds <= 0:
            raise ValueError("initial_seconds must be finite and > 0")
        if not isfinite(self.max_seconds) or self.max_seconds < self.initial_seconds:
            raise ValueError("max_seconds must be finite and >= initial_seconds")

    def delay_seconds(self, attempt: int) -> float:
        """Return a capped delay for a positive integer failure attempt."""
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        try:
            return min(self.max_seconds, ldexp(self.initial_seconds, attempt - 1))
        except OverflowError:
            return self.max_seconds
