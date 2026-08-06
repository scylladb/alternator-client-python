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

"""
MurmurHash3 x64 128-bit implementation for cross-language compatibility.

All implementations must produce identical hashes for key route affinity.
"""

from __future__ import annotations

# Type prefix bytes for AttributeValue encoding
# These must match the cross-language specification
TYPE_PREFIX_STRING = b"\x01"
TYPE_PREFIX_NUMBER = b"\x02"
TYPE_PREFIX_BINARY = b"\x03"

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

    DynamoDB partition keys only support S, N, and B types.

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
