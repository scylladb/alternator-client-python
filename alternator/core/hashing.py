"""
MurmurHash3 x64 128-bit implementation for cross-language compatibility.

All implementations must produce identical hashes for key route affinity.
"""

from __future__ import annotations

from typing import Any

# Type prefix bytes for AttributeValue encoding
# These must match the cross-language specification
TYPE_PREFIX_STRING = b"\x01"
TYPE_PREFIX_NUMBER = b"\x02"
TYPE_PREFIX_BINARY = b"\x03"
TYPE_PREFIX_BOOL = b"\x04"
TYPE_PREFIX_NULL = b"\x05"
TYPE_PREFIX_STRING_SET = b"\x06"
TYPE_PREFIX_NUMBER_SET = b"\x07"
TYPE_PREFIX_BINARY_SET = b"\x08"
TYPE_PREFIX_LIST = b"\x09"
TYPE_PREFIX_MAP = b"\x0a"

# Constants for MurmurHash3
_C1 = 0x87C37B91114253D5
_C2 = 0x4CF5AD432745937F
_MASK64 = 0xFFFFFFFFFFFFFFFF


def _rotl64(x: int, r: int) -> int:
    """Rotate left for 64-bit integer."""
    return ((x << r) | (x >> (64 - r))) & _MASK64


def _fmix64(k: int) -> int:
    """Final mixing function for 64-bit."""
    k ^= k >> 33
    k = (k * 0xFF51AFD7ED558CCD) & _MASK64
    k ^= k >> 33
    k = (k * 0xC4CEB9FE1A85EC53) & _MASK64
    k ^= k >> 33
    return k


def murmurhash3_x64_128(data: bytes, seed: int = 0) -> tuple[int, int]:
    """
    Compute MurmurHash3 x64 128-bit hash.

    Returns (h1, h2) where h1 is the first 64 bits.
    """
    length = len(data)
    nblocks = length // 16

    h1 = seed & _MASK64
    h2 = seed & _MASK64

    # Body - process 16-byte blocks
    for i in range(nblocks):
        offset = i * 16
        k1 = int.from_bytes(data[offset : offset + 8], "little")
        k2 = int.from_bytes(data[offset + 8 : offset + 16], "little")

        k1 = (k1 * _C1) & _MASK64
        k1 = _rotl64(k1, 31)
        k1 = (k1 * _C2) & _MASK64
        h1 ^= k1

        h1 = _rotl64(h1, 27)
        h1 = (h1 + h2) & _MASK64
        h1 = (h1 * 5 + 0x52DCE729) & _MASK64

        k2 = (k2 * _C2) & _MASK64
        k2 = _rotl64(k2, 33)
        k2 = (k2 * _C1) & _MASK64
        h2 ^= k2

        h2 = _rotl64(h2, 31)
        h2 = (h2 + h1) & _MASK64
        h2 = (h2 * 5 + 0x38495AB5) & _MASK64

    # Tail - handle remaining bytes
    tail = data[nblocks * 16 :]
    k1 = 0
    k2 = 0

    tail_len = len(tail)
    if tail_len >= 15:
        k2 ^= tail[14] << 48
    if tail_len >= 14:
        k2 ^= tail[13] << 40
    if tail_len >= 13:
        k2 ^= tail[12] << 32
    if tail_len >= 12:
        k2 ^= tail[11] << 24
    if tail_len >= 11:
        k2 ^= tail[10] << 16
    if tail_len >= 10:
        k2 ^= tail[9] << 8
    if tail_len >= 9:
        k2 ^= tail[8]
        k2 = (k2 * _C2) & _MASK64
        k2 = _rotl64(k2, 33)
        k2 = (k2 * _C1) & _MASK64
        h2 ^= k2

    if tail_len >= 8:
        k1 ^= tail[7] << 56
    if tail_len >= 7:
        k1 ^= tail[6] << 48
    if tail_len >= 6:
        k1 ^= tail[5] << 40
    if tail_len >= 5:
        k1 ^= tail[4] << 32
    if tail_len >= 4:
        k1 ^= tail[3] << 24
    if tail_len >= 3:
        k1 ^= tail[2] << 16
    if tail_len >= 2:
        k1 ^= tail[1] << 8
    if tail_len >= 1:
        k1 ^= tail[0]
        k1 = (k1 * _C1) & _MASK64
        k1 = _rotl64(k1, 31)
        k1 = (k1 * _C2) & _MASK64
        h1 ^= k1

    # Finalization
    h1 ^= length
    h2 ^= length

    h1 = (h1 + h2) & _MASK64
    h2 = (h2 + h1) & _MASK64

    h1 = _fmix64(h1)
    h2 = _fmix64(h2)

    h1 = (h1 + h2) & _MASK64
    h2 = (h2 + h1) & _MASK64

    return (h1, h2)


def hash_attribute_value(attr_type: str, value: str | bytes) -> int:
    """
    Hash a DynamoDB AttributeValue for key affinity routing.

    Args:
        attr_type: "S" (string), "N" (number), or "B" (binary)
        value: The attribute value

    Returns:
        First 64 bits of MurmurHash3 as signed int64
    """
    if attr_type == "S":
        prefix = TYPE_PREFIX_STRING
        data = value.encode("utf-8") if isinstance(value, str) else value
    elif attr_type == "N":
        prefix = TYPE_PREFIX_NUMBER
        # Validate numeric format before hashing
        str_value = str(value)
        try:
            float(str_value)
        except (ValueError, OverflowError) as exc:
            raise ValueError(
                f"Invalid numeric value for DynamoDB 'N' type: {value!r}"
            ) from exc
        data = str_value.encode("utf-8")
    elif attr_type == "B":
        prefix = TYPE_PREFIX_BINARY
        data = value if isinstance(value, bytes) else value.encode()
    else:
        raise ValueError(f"Unsupported attribute type: {attr_type}")

    h1, _ = murmurhash3_x64_128(prefix + data, seed=0)
    return _to_signed_int64(h1)


def _to_signed_int64(h: int) -> int:
    """Convert unsigned 64-bit int to signed int64."""
    if h >= 2**63:
        h -= 2**64
    return h


def _write_with_length(data: bytes) -> bytes:
    """Write a 4-byte big-endian length prefix followed by the data."""
    return len(data).to_bytes(4, "big") + data


def _encode_attribute_value(attr: dict[str, Any]) -> bytes:
    """
    Encode an AttributeValue into bytes following the cross-language specification.

    This matches the Go implementation at:
    https://github.com/scylladb/alternator-client-golang/blob/main/sdkv2/hasher.go
    """
    # String - prefix + raw data
    if "S" in attr:
        return TYPE_PREFIX_STRING + attr["S"].encode("utf-8")

    # Number - prefix + raw data
    if "N" in attr:
        return TYPE_PREFIX_NUMBER + str(attr["N"]).encode("utf-8")

    # Binary - prefix + raw data
    if "B" in attr:
        data = attr["B"] if isinstance(attr["B"], bytes) else attr["B"].encode()
        return TYPE_PREFIX_BINARY + data

    # Boolean - prefix + 0x01/0x00
    if "BOOL" in attr:
        return TYPE_PREFIX_BOOL + (b"\x01" if attr["BOOL"] else b"\x00")

    # NULL - prefix + 0x01
    if "NULL" in attr:
        if not attr["NULL"]:
            raise ValueError("NULL attribute must have true value")
        return TYPE_PREFIX_NULL + b"\x01"

    # String Set - prefix + sorted elements with length prefix
    if "SS" in attr:
        result = bytearray(TYPE_PREFIX_STRING_SET)
        # Sort strings lexicographically by UTF-8 bytes
        for s in sorted(attr["SS"]):
            result.extend(_write_with_length(s.encode("utf-8")))
        return bytes(result)

    # Number Set - prefix + sorted elements with length prefix
    if "NS" in attr:
        result = bytearray(TYPE_PREFIX_NUMBER_SET)
        # Sort number strings lexicographically (as strings)
        for n in sorted(attr["NS"]):
            result.extend(_write_with_length(str(n).encode("utf-8")))
        return bytes(result)

    # Binary Set - prefix + sorted elements with length prefix
    if "BS" in attr:
        result = bytearray(TYPE_PREFIX_BINARY_SET)
        # Sort byte arrays lexicographically
        elements = [b if isinstance(b, bytes) else b.encode() for b in attr["BS"]]
        for b in sorted(elements):
            result.extend(_write_with_length(b))
        return bytes(result)

    # List - prefix + ordered elements with length prefix (recursively encoded)
    if "L" in attr:
        result = bytearray(TYPE_PREFIX_LIST)
        for elem in attr["L"]:
            encoded = _encode_attribute_value(elem)
            result.extend(_write_with_length(encoded))
        return bytes(result)

    # Map - prefix + sorted keys with their values (both with length prefix)
    if "M" in attr:
        result = bytearray(TYPE_PREFIX_MAP)
        for key in sorted(attr["M"].keys()):
            result.extend(_write_with_length(key.encode("utf-8")))
            encoded_value = _encode_attribute_value(attr["M"][key])
            result.extend(_write_with_length(encoded_value))
        return bytes(result)

    raise ValueError(f"Unsupported attribute type: {attr}")


def hash_dynamodb_attribute(attr: dict[str, Any] | None) -> int:
    """
    Hash a DynamoDB AttributeValue dict for key affinity routing.

    Supports all DynamoDB types: S, N, B, BOOL, NULL, SS, NS, BS, L, M.
    This implementation follows the cross-language specification from
    https://github.com/scylladb/alternator-load-balancing/issues/165

    This is a public API function used for full AttributeValue hashing
    (as opposed to ``hash_attribute_value`` which handles only S/N/B types
    used in partition key routing).

    Args:
        attr: DynamoDB AttributeValue dict like {"S": "hello"} or None

    Returns:
        First 64 bits of MurmurHash3 as signed int64

    Raises:
        ValueError: For invalid attribute values (e.g., NULL with false value)
    """
    if attr is None:
        return 0

    encoded = _encode_attribute_value(attr)
    h1, _ = murmurhash3_x64_128(encoded, seed=0)
    return _to_signed_int64(h1)
