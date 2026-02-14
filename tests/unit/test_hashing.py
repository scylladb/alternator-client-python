"""Tests for MurmurHash3 implementation and hash vectors."""

import pytest

from alternator.core.hashing import (
    TYPE_PREFIX_BINARY,
    TYPE_PREFIX_BINARY_SET,
    TYPE_PREFIX_BOOL,
    TYPE_PREFIX_LIST,
    TYPE_PREFIX_MAP,
    TYPE_PREFIX_NULL,
    TYPE_PREFIX_NUMBER,
    TYPE_PREFIX_NUMBER_SET,
    TYPE_PREFIX_STRING,
    TYPE_PREFIX_STRING_SET,
    hash_attribute_value,
    hash_dynamodb_attribute,
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
        """Test empty string hash."""
        result = hash_attribute_value("S", "")
        assert result == 8849112093580131862

    def test_hello(self) -> None:
        """Test 'hello' hash."""
        result = hash_attribute_value("S", "hello")
        assert result == 8815023923555918238

    def test_user_123(self) -> None:
        """Test 'user_123' hash."""
        result = hash_attribute_value("S", "user_123")
        assert result == -4025731529809423594

    def test_unicode_japanese(self) -> None:
        """Test Japanese Unicode string hash."""
        result = hash_attribute_value("S", "こんにちは")
        assert result == -8746014667889746860


class TestHashAttributeValueNumberVectors:
    """Cross-language validation for Number type hash vectors."""

    def test_number_42(self) -> None:
        """Test number '42' hash."""
        result = hash_attribute_value("N", "42")
        assert result == -5061732451827723051

    def test_negative_number(self) -> None:
        """Test negative number '-12345' hash."""
        result = hash_attribute_value("N", "-12345")
        assert result == 2496798676881075539

    def test_decimal_number(self) -> None:
        """Test decimal '3.14159' hash."""
        result = hash_attribute_value("N", "3.14159")
        assert result == 2139945193071104172

    def test_scientific_notation(self) -> None:
        """Test scientific notation '1.23E10' hash."""
        result = hash_attribute_value("N", "1.23E10")
        assert result == -8571981415737439826


class TestHashAttributeValueBinaryVectors:
    """Cross-language validation for Binary type hash vectors."""

    def test_empty_binary(self) -> None:
        """Test empty binary hash."""
        result = hash_attribute_value("B", b"")
        assert result == 8244620721157455449

    def test_binary_010203(self) -> None:
        """Test binary 0x01 0x02 0x03 hash."""
        result = hash_attribute_value("B", bytes([0x01, 0x02, 0x03]))
        assert result == 5026299041734804437

    def test_binary_ff0080(self) -> None:
        """Test binary 0xFF 0x00 0x80 hash."""
        result = hash_attribute_value("B", bytes([0xFF, 0x00, 0x80]))
        assert result == 14533934253577680


class TestTypeCollisionPrevention:
    """Tests that same content with different types produce different hashes."""

    def test_string_vs_number_42(self) -> None:
        """Test that '42' as string != '42' as number."""
        string_hash = hash_attribute_value("S", "42")
        number_hash = hash_attribute_value("N", "42")
        assert string_hash != number_hash

    def test_string_vs_binary_hello(self) -> None:
        """Test that 'hello' as string != 'hello' as binary."""
        string_hash = hash_attribute_value("S", "hello")
        binary_hash = hash_attribute_value("B", b"hello")
        assert string_hash != binary_hash

    def test_empty_string_vs_empty_binary(self) -> None:
        """Test that empty string != empty binary."""
        string_hash = hash_attribute_value("S", "")
        binary_hash = hash_attribute_value("B", b"")
        assert string_hash != binary_hash


class TestTypePrefixConstants:
    """Tests for type prefix constants."""

    def test_string_prefix(self) -> None:
        """Test string type prefix."""
        assert TYPE_PREFIX_STRING == b"\x01"

    def test_number_prefix(self) -> None:
        """Test number type prefix."""
        assert TYPE_PREFIX_NUMBER == b"\x02"

    def test_binary_prefix(self) -> None:
        """Test binary type prefix."""
        assert TYPE_PREFIX_BINARY == b"\x03"

    def test_prefixes_are_different(self) -> None:
        """Test all prefixes are unique."""
        prefixes = [TYPE_PREFIX_STRING, TYPE_PREFIX_NUMBER, TYPE_PREFIX_BINARY]
        assert len(set(prefixes)) == 3


class TestHashAttributeValueEdgeCases:
    """Edge case tests for hash_attribute_value."""

    def test_invalid_type_raises(self) -> None:
        """Test invalid type raises ValueError."""
        with pytest.raises(ValueError, match="Unsupported attribute type"):
            hash_attribute_value("X", "value")

    def test_long_string(self) -> None:
        """Test hashing a long string."""
        long_str = "a" * 10000
        result = hash_attribute_value("S", long_str)
        assert isinstance(result, int)

    def test_special_characters(self) -> None:
        """Test hashing special characters."""
        result = hash_attribute_value("S", "!@#$%^&*()")
        assert isinstance(result, int)

    def test_newlines_and_tabs(self) -> None:
        """Test hashing strings with whitespace."""
        result = hash_attribute_value("S", "line1\nline2\ttab")
        assert isinstance(result, int)

    def test_null_bytes_in_binary(self) -> None:
        """Test binary with null bytes."""
        result = hash_attribute_value("B", b"\x00\x00\x00")
        assert isinstance(result, int)

    def test_result_is_signed_int64(self) -> None:
        """Test result is within signed int64 range."""
        test_values = ["test1", "test2", "test3", "long" * 100]
        for val in test_values:
            result = hash_attribute_value("S", val)
            assert -(2**63) <= result < 2**63


# =============================================================================
# Tests for hash_dynamodb_attribute - matching Go hasher_test.go coverage
# =============================================================================


class TestHashDynamoDBAttribute_Nil:
    """Test that None input produces hash value of 0."""

    def test_nil_returns_zero(self) -> None:
        """Test nil input produces hash value of 0."""
        result = hash_dynamodb_attribute(None)
        assert result == 0


class TestHashDynamoDBAttribute_PrimitiveTypes:
    """Test vectors for primitive types matching Go implementation."""

    # String tests
    def test_string_hello(self) -> None:
        result = hash_dynamodb_attribute({"S": "hello"})
        assert result == 8815023923555918238

    def test_string_empty(self) -> None:
        result = hash_dynamodb_attribute({"S": ""})
        assert result == 8849112093580131862

    def test_string_user_123(self) -> None:
        result = hash_dynamodb_attribute({"S": "user_123"})
        assert result == -4025731529809423594

    def test_string_unicode(self) -> None:
        result = hash_dynamodb_attribute({"S": "こんにちは"})
        assert result == -8746014667889746860

    # Number tests
    def test_number_42(self) -> None:
        result = hash_dynamodb_attribute({"N": "42"})
        assert result == -5061732451827723051

    def test_number_negative(self) -> None:
        result = hash_dynamodb_attribute({"N": "-12345"})
        assert result == 2496798676881075539

    def test_number_decimal(self) -> None:
        result = hash_dynamodb_attribute({"N": "3.14159"})
        assert result == 2139945193071104172

    def test_number_scientific(self) -> None:
        result = hash_dynamodb_attribute({"N": "1.23E10"})
        assert result == -8571981415737439826

    # Binary tests
    def test_binary_123(self) -> None:
        result = hash_dynamodb_attribute({"B": bytes([0x01, 0x02, 0x03])})
        assert result == 5026299041734804437

    def test_binary_empty(self) -> None:
        result = hash_dynamodb_attribute({"B": b""})
        assert result == 8244620721157455449

    def test_binary_ff0080(self) -> None:
        result = hash_dynamodb_attribute({"B": bytes([0xFF, 0x00, 0x80])})
        assert result == 14533934253577680

    # Boolean tests
    def test_bool_true(self) -> None:
        result = hash_dynamodb_attribute({"BOOL": True})
        assert result == 8486936384116756332

    def test_bool_false(self) -> None:
        result = hash_dynamodb_attribute({"BOOL": False})
        assert result == -4126391008895418907

    # NULL test
    def test_null_true(self) -> None:
        result = hash_dynamodb_attribute({"NULL": True})
        assert result == -561667943985901489


class TestHashDynamoDBAttribute_CollectionTypes:
    """Test vectors for collection types matching Go implementation."""

    # String Set tests - should produce same hash regardless of input order
    def test_string_set_ordered(self) -> None:
        result = hash_dynamodb_attribute({"SS": ["a", "b", "c"]})
        assert result == 7306159961466191513

    def test_string_set_unordered(self) -> None:
        result = hash_dynamodb_attribute({"SS": ["c", "a", "b"]})
        assert result == 7306159961466191513

    def test_string_set_empty(self) -> None:
        result = hash_dynamodb_attribute({"SS": []})
        assert result == 1389283912212466035

    # Number Set tests - should produce same hash regardless of input order
    def test_number_set_ordered(self) -> None:
        result = hash_dynamodb_attribute({"NS": ["1", "2", "3"]})
        assert result == 7671176432463372843

    def test_number_set_unordered(self) -> None:
        result = hash_dynamodb_attribute({"NS": ["3", "1", "2"]})
        assert result == 7671176432463372843

    # Binary Set tests
    def test_binary_set(self) -> None:
        result = hash_dynamodb_attribute({"BS": [bytes([0x01]), bytes([0x02])]})
        assert result == 1665953200922610785

    # List tests
    def test_list_mixed(self) -> None:
        result = hash_dynamodb_attribute({"L": [{"S": "a"}, {"N": "1"}]})
        assert result == 2820707766025454319

    def test_list_empty(self) -> None:
        result = hash_dynamodb_attribute({"L": []})
        assert result == -9218108584195748763

    # Map tests
    def test_map_age_name(self) -> None:
        result = hash_dynamodb_attribute(
            {
                "M": {
                    "age": {"N": "30"},
                    "name": {"S": "John"},
                }
            }
        )
        assert result == -902430298826217654

    def test_map_empty(self) -> None:
        result = hash_dynamodb_attribute({"M": {}})
        assert result == 3924702969362948632

    def test_map_nested_list(self) -> None:
        result = hash_dynamodb_attribute({"M": {"key": {"L": [{"S": "nested"}]}}})
        assert result == -5371960927743395604


class TestHashDynamoDBAttribute_TypeCollisionPrevention:
    """Test that different types with same underlying data produce different hashes."""

    def test_string_12345(self) -> None:
        result = hash_dynamodb_attribute({"S": "12345"})
        assert result == -6122888897254035317

    def test_number_12345(self) -> None:
        result = hash_dynamodb_attribute({"N": "12345"})
        assert result == -3190731486301745196

    def test_binary_12345(self) -> None:
        result = hash_dynamodb_attribute({"B": b"12345"})
        assert result == -3752463870508600385

    def test_all_different(self) -> None:
        """Verify all three types produce different hashes."""
        hash_s = hash_dynamodb_attribute({"S": "12345"})
        hash_n = hash_dynamodb_attribute({"N": "12345"})
        hash_b = hash_dynamodb_attribute({"B": b"12345"})

        assert hash_s != hash_n
        assert hash_s != hash_b
        assert hash_n != hash_b


class TestHashDynamoDBAttribute_BoundaryCollisionPrevention:
    """Test that different element boundaries produce different hashes."""

    def test_string_set_a_bc(self) -> None:
        result = hash_dynamodb_attribute({"SS": ["a", "bc"]})
        assert result == 1290520225009436005

    def test_string_set_ab_c(self) -> None:
        result = hash_dynamodb_attribute({"SS": ["ab", "c"]})
        assert result == -5535761315402902992

    def test_list_a_bc(self) -> None:
        result = hash_dynamodb_attribute({"L": [{"S": "a"}, {"S": "bc"}]})
        assert result == -8510235581865967010

    def test_list_ab_c(self) -> None:
        result = hash_dynamodb_attribute({"L": [{"S": "ab"}, {"S": "c"}]})
        assert result == 1154309738056842165


class TestHashDynamoDBAttribute_NullFalseError:
    """Test that NULL with false value raises an error."""

    def test_null_false_raises(self) -> None:
        with pytest.raises(ValueError, match="NULL attribute must have true value"):
            hash_dynamodb_attribute({"NULL": False})


class TestHashDynamoDBAttribute_SetOrderIndependence:
    """Verify that sets produce same hash regardless of input order."""

    def test_string_set_order_independence(self) -> None:
        ordered = hash_dynamodb_attribute({"SS": ["apple", "banana", "cherry"]})
        unordered = hash_dynamodb_attribute({"SS": ["cherry", "apple", "banana"]})
        assert ordered == unordered

    def test_number_set_order_independence(self) -> None:
        ordered = hash_dynamodb_attribute({"NS": ["100", "200", "300"]})
        unordered = hash_dynamodb_attribute({"NS": ["300", "100", "200"]})
        assert ordered == unordered

    def test_binary_set_order_independence(self) -> None:
        ordered = hash_dynamodb_attribute(
            {"BS": [bytes([0x01]), bytes([0x02]), bytes([0x03])]}
        )
        unordered = hash_dynamodb_attribute(
            {"BS": [bytes([0x03]), bytes([0x01]), bytes([0x02])]}
        )
        assert ordered == unordered


class TestHashDynamoDBAttribute_MapKeyOrder:
    """Verify maps produce consistent hashes regardless of iteration order."""

    def test_map_consistent_hash(self) -> None:
        m = {
            "M": {
                "z": {"S": "last"},
                "a": {"S": "first"},
                "m": {"S": "middle"},
            }
        }

        # Hash multiple times - should always be the same
        hash1 = hash_dynamodb_attribute(m)
        hash2 = hash_dynamodb_attribute(m)
        hash3 = hash_dynamodb_attribute(m)

        assert hash1 == hash2
        assert hash2 == hash3


class TestHashDynamoDBAttribute_ListOrderDependence:
    """Verify lists produce different hashes for different orders."""

    def test_list_order_matters(self) -> None:
        list1 = hash_dynamodb_attribute({"L": [{"S": "a"}, {"S": "b"}]})
        list2 = hash_dynamodb_attribute({"L": [{"S": "b"}, {"S": "a"}]})

        assert list1 != list2


class TestTypePrefixConstantsExtended:
    """Tests for all type prefix constants matching Go implementation."""

    def test_bool_prefix(self) -> None:
        assert TYPE_PREFIX_BOOL == b"\x04"

    def test_null_prefix(self) -> None:
        assert TYPE_PREFIX_NULL == b"\x05"

    def test_string_set_prefix(self) -> None:
        assert TYPE_PREFIX_STRING_SET == b"\x06"

    def test_number_set_prefix(self) -> None:
        assert TYPE_PREFIX_NUMBER_SET == b"\x07"

    def test_binary_set_prefix(self) -> None:
        assert TYPE_PREFIX_BINARY_SET == b"\x08"

    def test_list_prefix(self) -> None:
        assert TYPE_PREFIX_LIST == b"\x09"

    def test_map_prefix(self) -> None:
        assert TYPE_PREFIX_MAP == b"\x0a"

    def test_all_prefixes_unique(self) -> None:
        prefixes = [
            TYPE_PREFIX_STRING,
            TYPE_PREFIX_NUMBER,
            TYPE_PREFIX_BINARY,
            TYPE_PREFIX_BOOL,
            TYPE_PREFIX_NULL,
            TYPE_PREFIX_STRING_SET,
            TYPE_PREFIX_NUMBER_SET,
            TYPE_PREFIX_BINARY_SET,
            TYPE_PREFIX_LIST,
            TYPE_PREFIX_MAP,
        ]
        assert len(set(prefixes)) == len(prefixes)


class TestHashDynamoDBAttribute_EdgeCases:
    """Edge case tests for hash_dynamodb_attribute."""

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="Unsupported attribute type"):
            hash_dynamodb_attribute({"X": "value"})

    def test_deeply_nested_structure(self) -> None:
        """Test deeply nested map/list structure."""
        nested = {"M": {"level1": {"L": [{"M": {"level2": {"S": "deep"}}}]}}}
        result = hash_dynamodb_attribute(nested)
        assert isinstance(result, int)
        assert -(2**63) <= result < 2**63

    def test_result_is_signed_int64(self) -> None:
        """Test result is within signed int64 range."""
        test_cases = [
            {"S": "test"},
            {"N": "42"},
            {"B": b"binary"},
            {"BOOL": True},
            {"NULL": True},
            {"SS": ["a", "b"]},
            {"NS": ["1", "2"]},
            {"BS": [b"\x01"]},
            {"L": [{"S": "item"}]},
            {"M": {"key": {"S": "value"}}},
        ]
        for attr in test_cases:
            result = hash_dynamodb_attribute(attr)
            assert -(2**63) <= result < 2**63
