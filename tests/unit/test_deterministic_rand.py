"""Tests for deterministic query-plan PRNG."""

from __future__ import annotations

import pytest

from alternator.core.deterministic_rand import DeterministicRand

MAX_INT64 = (1 << 63) - 1


class TestDeterministicRandSeedEdgeCases:
    """Test seed edge cases."""

    def test_seed_zero_replaced(self) -> None:
        """Seed 0 is replaced with 89482311 internally."""
        r = DeterministicRand(0)
        # Should not raise and should produce valid output
        val = r.int63()
        assert 0 <= val < (1 << 63)

    def test_negative_seed(self) -> None:
        """Negative seeds work and produce deterministic output."""
        r1 = DeterministicRand(-1)
        r2 = DeterministicRand(-1)
        assert [r1.int63() for _ in range(10)] == [r2.int63() for _ in range(10)]

    def test_max_int64_seed(self) -> None:
        """MAX_INT64 seed works."""
        r = DeterministicRand(MAX_INT64)
        val = r.int63()
        assert 0 <= val < (1 << 63)


class TestDeterministicRandInt63:
    """Test int63() output matches stable reference values."""

    def test_matches_stable_reference(self) -> None:
        """First 10 int63() values for seed 42 must match stable vectors."""
        expected = [
            3440579354231278675,
            608747136543856411,
            5571782338101878760,
            1926012586526624009,
            404153945743547657,
            3534334367214237261,
            7497468244883513247,
            3545887102062614208,
            3532963341805492868,
            5961769557461764184,
        ]
        r = DeterministicRand(42)
        actual = [r.int63() for _ in range(10)]
        assert actual == expected

    def test_deterministic(self) -> None:
        """Same seed produces same sequence."""
        r1 = DeterministicRand(42)
        r2 = DeterministicRand(42)
        for _ in range(20):
            assert r1.int63() == r2.int63()

    def test_range(self) -> None:
        """int63() returns values in [0, 2^63)."""
        r = DeterministicRand(42)
        for _ in range(100):
            val = r.int63()
            assert 0 <= val < (1 << 63)


class TestDeterministicRandIntn:
    """Test intn() correctness."""

    def test_intn_range(self) -> None:
        """intn(n) returns values in [0, n)."""
        r = DeterministicRand(42)
        for n in [1, 2, 3, 10, 100, 1000]:
            for _ in range(50):
                val = r.intn(n)
                assert 0 <= val < n

    def test_intn_one_always_zero(self) -> None:
        """intn(1) always returns 0."""
        r = DeterministicRand(42)
        for _ in range(20):
            assert r.intn(1) == 0

    def test_intn_invalid(self) -> None:
        """intn with n<=0 raises ValueError."""
        r = DeterministicRand(42)
        with pytest.raises(ValueError):
            r.intn(0)
        with pytest.raises(ValueError):
            r.intn(-1)

    def test_intn_large_n(self) -> None:
        """intn works for n > 2^31-1 (uses int63n path)."""
        r = DeterministicRand(42)
        n = (1 << 32) + 1
        for _ in range(20):
            val = r.intn(n)
            assert 0 <= val < n

    def test_intn_deterministic(self) -> None:
        """Same seed produces same intn sequence."""
        r1 = DeterministicRand(123)
        r2 = DeterministicRand(123)
        for _ in range(20):
            assert r1.intn(100) == r2.intn(100)
