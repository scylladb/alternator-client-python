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

"""Unit tests for ScyllaDB version detection."""

import os
from unittest import mock

from tests.integration.scylla_version import (
    GZIP_COMPRESSION_MIN_VERSION,
    ScyllaVersion,
    clear_version_cache,
    get_version_from_env,
    requires_scylla_version,
    supports_gzip_compression,
)


class TestScyllaVersion:
    """Tests for ScyllaVersion parsing and comparison."""

    def test_parse_simple_version(self) -> None:
        """Test parsing simple version strings."""
        version = ScyllaVersion.parse("2025.4.0")
        assert version is not None
        assert version.major == 2025
        assert version.minor == 4
        assert version.patch == 0

    def test_parse_version_without_patch(self) -> None:
        """Test parsing version without patch number."""
        version = ScyllaVersion.parse("2025.4")
        assert version is not None
        assert version.major == 2025
        assert version.minor == 4
        assert version.patch == 0

    def test_parse_version_with_rc_suffix(self) -> None:
        """Test parsing version with release candidate suffix."""
        version = ScyllaVersion.parse("2026.1.0-rc0")
        assert version is not None
        assert version.major == 2026
        assert version.minor == 1
        assert version.patch == 0

    def test_parse_version_with_tilde_suffix(self) -> None:
        """Test parsing version with tilde suffix."""
        version = ScyllaVersion.parse("2025.4.0~rc1")
        assert version is not None
        assert version.major == 2025
        assert version.minor == 4
        assert version.patch == 0

    def test_parse_version_with_build_suffix(self) -> None:
        """Test parsing version with build number suffix."""
        version = ScyllaVersion.parse("5.4.0-0.20240101")
        assert version is not None
        assert version.major == 5
        assert version.minor == 4
        assert version.patch == 0

    def test_parse_old_style_version(self) -> None:
        """Test parsing old-style version (6.x)."""
        version = ScyllaVersion.parse("6.2.3")
        assert version is not None
        assert version.major == 6
        assert version.minor == 2
        assert version.patch == 3

    def test_parse_invalid_version(self) -> None:
        """Test parsing invalid version strings."""
        assert ScyllaVersion.parse("") is None
        assert ScyllaVersion.parse("invalid") is None
        assert ScyllaVersion.parse("abc.def.ghi") is None

    def test_parse_whitespace(self) -> None:
        """Test parsing version with surrounding whitespace."""
        version = ScyllaVersion.parse("  2025.4.0  ")
        assert version is not None
        assert version.major == 2025

    def test_version_comparison(self) -> None:
        """Test version comparison operators."""
        v2025_4 = ScyllaVersion(2025, 4, 0)
        v2025_4_1 = ScyllaVersion(2025, 4, 1)
        v2026_1 = ScyllaVersion(2026, 1, 0)
        v6_2 = ScyllaVersion(6, 2, 0)

        # Less than
        assert v2025_4 < v2025_4_1
        assert v2025_4 < v2026_1
        assert v6_2 < v2025_4  # Old style < new style

        # Greater than
        assert v2026_1 > v2025_4
        assert v2025_4_1 > v2025_4

        # Equal
        assert v2025_4 == ScyllaVersion(2025, 4, 0)

        # Greater than or equal
        assert v2026_1 >= v2025_4
        assert v2025_4 >= v2025_4

    def test_version_str(self) -> None:
        """Test version string representation."""
        version = ScyllaVersion(2025, 4, 2)
        assert str(version) == "2025.4.2"


class TestVersionFromEnv:
    """Tests for environment variable version detection."""

    def test_get_version_from_env_set(self) -> None:
        """Test getting version from environment variable."""
        clear_version_cache()
        with mock.patch.dict(os.environ, {"SCYLLA_VERSION": "2026.1.0"}):
            version = get_version_from_env()
            assert version is not None
            assert version == ScyllaVersion(2026, 1, 0)

    def test_get_version_from_env_not_set(self) -> None:
        """Test getting version when env var not set."""
        clear_version_cache()
        with mock.patch.dict(os.environ, {"SCYLLA_VERSION": ""}):
            version = get_version_from_env()
            assert version is None

    def test_get_version_from_env_invalid(self) -> None:
        """Test getting version with invalid env var value."""
        clear_version_cache()
        with mock.patch.dict(os.environ, {"SCYLLA_VERSION": "invalid"}):
            version = get_version_from_env()
            assert version is None


class TestSupportsGzipCompression:
    """Tests for gzip compression version check."""

    def test_supports_gzip_2026_1_0(self) -> None:
        """Test that 2026.1.0 supports gzip compression."""
        version = ScyllaVersion(2026, 1, 0)
        assert supports_gzip_compression(version) is True

    def test_supports_gzip_2026_1_1(self) -> None:
        """Test that 2026.1.1 supports gzip compression."""
        version = ScyllaVersion(2026, 1, 1)
        assert supports_gzip_compression(version) is True

    def test_supports_gzip_2026_2_0(self) -> None:
        """Test that 2026.2.0 supports gzip compression."""
        version = ScyllaVersion(2026, 2, 0)
        assert supports_gzip_compression(version) is True

    def test_not_supports_gzip_2025_4_0(self) -> None:
        """Test that 2025.4.0 does not support gzip compression."""
        version = ScyllaVersion(2025, 4, 0)
        assert supports_gzip_compression(version) is False

    def test_not_supports_gzip_2025_x(self) -> None:
        """Test that any 2025.x version does not support gzip compression."""
        for minor in range(1, 5):
            version = ScyllaVersion(2025, minor, 0)
            assert supports_gzip_compression(version) is False

    def test_not_supports_gzip_none(self) -> None:
        """Test that unknown version (None) returns False."""
        assert supports_gzip_compression(None) is False

    def test_gzip_min_version_constant(self) -> None:
        """Test that the minimum version constant is correct."""
        assert ScyllaVersion(2026, 1, 0) == GZIP_COMPRESSION_MIN_VERSION


class TestRequiresScyllaVersion:
    """Tests for skip reason generation."""

    def test_requires_version_with_feature(self) -> None:
        """Test skip reason with feature name."""
        reason = requires_scylla_version(ScyllaVersion(2026, 1, 0), "gzip compression")
        assert "gzip compression" in reason
        assert "2026.1.0" in reason

    def test_requires_version_without_feature(self) -> None:
        """Test skip reason without feature name."""
        reason = requires_scylla_version(ScyllaVersion(2026, 1, 0))
        assert "2026.1.0" in reason
        assert "Requires" in reason
