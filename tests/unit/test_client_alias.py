"""Tests for top-level client convenience alias."""

import pytest

import alternator
from alternator import AlternatorClient, Auth, Config
from alternator.client import DEFAULT_PORT, client
from alternator.exceptions import ConfigurationError


class TestClientAlias:
    """Tests for alternator.client convenience API."""

    def test_builds_context_manager_from_seed_keywords(self) -> None:
        """The alias builds an AlternatorClient from host-only seeds."""
        ctx = client(seeds=["node1", "node2"])

        assert isinstance(ctx, AlternatorClient)
        assert ctx._config.seed_hosts == ("node1", "node2")
        assert ctx._config.port == DEFAULT_PORT
        assert ctx._config.scheme == "http"

    def test_accepts_positional_seeds(self) -> None:
        """The alias also accepts seeds as the first argument."""
        ctx = client(["node1"], port=8042, scheme="https")

        assert ctx._config.seed_hosts == ("node1",)
        assert ctx._config.port == 8042
        assert ctx._config.scheme == "https"

    def test_accepts_config_object(self) -> None:
        """The alias can wrap an existing Config."""
        config = Config(seed_hosts=["node1"], port=9000)

        ctx = client(config, auth=Auth.disabled(), region_name="us-east-1")

        assert ctx._config is config
        assert ctx._auth == Auth.disabled()
        assert ctx._boto_kwargs["region_name"] == "us-east-1"

    def test_rejects_missing_seeds(self) -> None:
        """Seeds are required unless a Config object is passed."""
        with pytest.raises(ConfigurationError, match="seeds is required"):
            client()

    def test_rejects_host_port_seed(self) -> None:
        """Seeds must not include ports."""
        with pytest.raises(ConfigurationError, match="without ports"):
            client(seeds=["node1:8000"])

    def test_rejects_url_seed(self) -> None:
        """Seeds must not include URL schemes."""
        with pytest.raises(ConfigurationError, match="without ports"):
            client(seeds=["http://node1"])

    def test_rejects_config_with_port_override(self) -> None:
        """A Config object cannot be mixed with direct port settings."""
        config = Config(seed_hosts=["node1"], port=9000)

        with pytest.raises(ConfigurationError, match="Config object"):
            client(config, port=8001)

    def test_top_level_export(self) -> None:
        """Top-level alternator.client is the convenience alias."""
        assert alternator.client is client
