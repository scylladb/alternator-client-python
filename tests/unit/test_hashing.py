"""Tests for MurmurHash3 implementation and hash vectors."""

import pytest

from alternator.core.hashing import (
    TYPE_PREFIX_BINARY,
    TYPE_PREFIX_NUMBER,
    TYPE_PREFIX_STRING,
    hash_attribute_value,
    murmurhash3_x64_128,
)


class TestMurmurHash3:
    """Tests for murmurhash3_x64_128 function."""

    def test_empty_data(self) -> None:
        """Test hashing empty bytes."""
        h1, h2 = murmurhash3_x64_128(b"", seed=0)
        # Should produce consistent hash for empty input
        assert isinstance(h1, int)
        assert isinstance(h2, int)

    def test_returns_tuple_of_ints(self) -> None:
        """Test return type is tuple of two ints."""
        result = murmurhash3_x64_128(b"test data")
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert all(isinstance(x, int) for x in result)

    def test_deterministic(self) -> None:
        """Test same input produces same hash."""
        data = b"hello world"
        h1, h2 = murmurhash3_x64_128(data)
        h1_again, h2_again = murmurhash3_x64_128(data)
        assert h1 == h1_again
        assert h2 == h2_again

    def test_different_seeds_produce_different_hashes(self) -> None:
        """Test different seeds produce different hashes."""
        data = b"test"
        h1_seed0, _ = murmurhash3_x64_128(data, seed=0)
        h1_seed1, _ = murmurhash3_x64_128(data, seed=1)
        assert h1_seed0 != h1_seed1


class TestHashAttributeValueStringVectors:
    """Cross-language validation for String type hash vectors."""

    def test_empty_string(self) -> None:
        result = hash_attribute_value("S", "")
        assert result == 8849112093580131862

    def test_hello(self) -> None:
        result = hash_attribute_value("S", "hello")
        assert result == 8815023923555918238

    def test_user_123(self) -> None:
        result = hash_attribute_value("S", "user_123")
        assert result == -4025731529809423594

    def test_unicode_japanese(self) -> None:
        result = hash_attribute_value("S", "こんにちは")
        assert result == -8746014667889746860


class TestHashAttributeValueNumberVectors:
    """Cross-language validation for Number type hash vectors."""

    def test_number_42(self) -> None:
        result = hash_attribute_value("N", "42")
        assert result == -5061732451827723051

    def test_negative_number(self) -> None:
        result = hash_attribute_value("N", "-12345")
        assert result == 2496798676881075539

    def test_decimal_number(self) -> None:
        result = hash_attribute_value("N", "3.14159")
        assert result == 2139945193071104172

    def test_scientific_notation(self) -> None:
        result = hash_attribute_value("N", "1.23E10")
        assert result == -8571981415737439826


class TestHashAttributeValueBinaryVectors:
    """Cross-language validation for Binary type hash vectors."""

    def test_empty_binary(self) -> None:
        result = hash_attribute_value("B", b"")
        assert result == 8244620721157455449

    def test_binary_010203(self) -> None:
        result = hash_attribute_value("B", bytes([0x01, 0x02, 0x03]))
        assert result == 5026299041734804437

    def test_binary_ff0080(self) -> None:
        result = hash_attribute_value("B", bytes([0xFF, 0x00, 0x80]))
        assert result == 14533934253577680


class TestTypeCollisionPrevention:
    """Tests that same content with different types produce different hashes."""

    def test_string_vs_number_42(self) -> None:
        string_hash = hash_attribute_value("S", "42")
        number_hash = hash_attribute_value("N", "42")
        assert string_hash != number_hash

    def test_string_vs_binary_hello(self) -> None:
        string_hash = hash_attribute_value("S", "hello")
        binary_hash = hash_attribute_value("B", b"hello")
        assert string_hash != binary_hash

    def test_empty_string_vs_empty_binary(self) -> None:
        string_hash = hash_attribute_value("S", "")
        binary_hash = hash_attribute_value("B", b"")
        assert string_hash != binary_hash


class TestTypePrefixConstants:
    """Tests for type prefix constants."""

    def test_string_prefix(self) -> None:
        assert TYPE_PREFIX_STRING == b"\x01"

    def test_number_prefix(self) -> None:
        assert TYPE_PREFIX_NUMBER == b"\x02"

    def test_binary_prefix(self) -> None:
        assert TYPE_PREFIX_BINARY == b"\x03"

    def test_prefixes_are_different(self) -> None:
        prefixes = [TYPE_PREFIX_STRING, TYPE_PREFIX_NUMBER, TYPE_PREFIX_BINARY]
        assert len(set(prefixes)) == 3


class TestHashAttributeValueEdgeCases:
    """Edge case tests for hash_attribute_value."""

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported attribute type"):
            hash_attribute_value("X", "value")

    def test_unsupported_bool_type_raises(self) -> None:
        """BOOL is not a valid partition key type."""
        with pytest.raises(ValueError, match="Unsupported attribute type"):
            hash_attribute_value("BOOL", "true")

    def test_long_string(self) -> None:
        long_str = "a" * 10000
        result = hash_attribute_value("S", long_str)
        assert isinstance(result, int)

    def test_special_characters(self) -> None:
        result = hash_attribute_value("S", "!@#$%^&*()")
        assert isinstance(result, int)

    def test_newlines_and_tabs(self) -> None:
        result = hash_attribute_value("S", "line1\nline2\ttab")
        assert isinstance(result, int)

    def test_null_bytes_in_binary(self) -> None:
        result = hash_attribute_value("B", b"\x00\x00\x00")
        assert isinstance(result, int)

    def test_result_is_signed_int64(self) -> None:
        test_values = ["test1", "test2", "test3", "long" * 100]
        for val in test_values:
            result = hash_attribute_value("S", val)
            assert -(2**63) <= result < 2**63
