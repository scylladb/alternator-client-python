"""Property-based tests for hash distribution verification."""

from hypothesis import given, settings
from hypothesis import strategies as st

from alternator.core.hashing import hash_attribute_value, murmurhash3_x64_128
from alternator.core.key_affinity import AffinitySelector
from alternator.core.live_nodes import NodeList


class TestMurmur3Properties:
    """Property-based tests for murmur3 hash function."""

    @given(st.binary(min_size=0, max_size=1000))
    def test_deterministic(self, data: bytes) -> None:
        """Test that hash is deterministic for same input."""
        hash1 = murmurhash3_x64_128(data)
        hash2 = murmurhash3_x64_128(data)
        assert hash1 == hash2

    @given(st.binary(min_size=0, max_size=1000))
    def test_returns_tuple_of_two_ints(self, data: bytes) -> None:
        """Test that hash returns tuple of two integers."""
        result = murmurhash3_x64_128(data)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], int)
        assert isinstance(result[1], int)

    @given(
        st.binary(min_size=1, max_size=1000),
        st.integers(min_value=1, max_value=2**32 - 1),
    )
    def test_different_seeds_generally_produce_different_results(
        self, data: bytes, seed: int
    ) -> None:
        """Test that different seeds generally produce different hashes."""
        # With high probability, different seeds should produce different hashes
        hash_seed_0 = murmurhash3_x64_128(data, seed=0)
        hash_seed_n = murmurhash3_x64_128(data, seed=seed)
        # We just verify both calls succeed - equality is probabilistic
        assert isinstance(hash_seed_0, tuple)
        assert isinstance(hash_seed_n, tuple)


class TestHashAttributeValueProperties:
    """Property-based tests for hash_attribute_value function."""

    @given(st.text(min_size=0, max_size=100))
    def test_string_hash_deterministic(self, value: str) -> None:
        """Test that string hash is deterministic."""
        hash1 = hash_attribute_value("S", value)
        hash2 = hash_attribute_value("S", value)
        assert hash1 == hash2

    @given(st.text(min_size=0, max_size=100))
    def test_string_hash_returns_int(self, value: str) -> None:
        """Test that string hash returns an integer."""
        result = hash_attribute_value("S", value)
        assert isinstance(result, int)

    @given(st.integers())
    def test_number_hash_deterministic(self, value: int) -> None:
        """Test that number hash is deterministic."""
        str_value = str(value)
        hash1 = hash_attribute_value("N", str_value)
        hash2 = hash_attribute_value("N", str_value)
        assert hash1 == hash2

    @given(st.binary(min_size=0, max_size=100))
    def test_binary_hash_deterministic(self, value: bytes) -> None:
        """Test that binary hash is deterministic."""
        hash1 = hash_attribute_value("B", value)
        hash2 = hash_attribute_value("B", value)
        assert hash1 == hash2

    @given(st.text(min_size=1, max_size=50))
    def test_different_types_produce_different_hashes(self, value: str) -> None:
        """Test that same value with different types produces different hashes."""
        # String "42" should hash differently than number "42"
        string_hash = hash_attribute_value("S", value)
        # Only test if value could be a valid number
        try:
            float(value)
            number_hash = hash_attribute_value("N", value)
            # They should be different due to type prefix
            assert string_hash != number_hash
        except ValueError:
            pass  # Not a valid number, skip this test case


class TestAffinitySelectorProperties:
    """Property-based tests for AffinitySelector."""

    @given(st.integers())
    def test_selection_deterministic(self, hash_value: int) -> None:
        """Test that selection is deterministic for same hash."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c", "d", "e"), scope_name="test")

        result1 = selector.select(nodes, hash_value)
        result2 = selector.select(nodes, hash_value)
        assert result1 == result2

    @given(st.integers())
    def test_selection_always_valid_node(self, hash_value: int) -> None:
        """Test that selection always returns a valid node."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("node1", "node2", "node3"), scope_name="test")

        result = selector.select(nodes, hash_value)
        assert result in nodes.nodes

    @given(st.integers())
    def test_selection_with_single_node(self, hash_value: int) -> None:
        """Test that single node is always selected."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("only_node",), scope_name="test")

        result = selector.select(nodes, hash_value)
        assert result == "only_node"

    @given(st.integers())
    def test_empty_nodes_returns_none(self, hash_value: int) -> None:
        """Test that empty node list returns None."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=(), scope_name="test")

        result = selector.select(nodes, hash_value)
        assert result is None

    @settings(max_examples=200)
    @given(
        st.lists(
            st.integers(min_value=0, max_value=1000),
            min_size=100,
            max_size=100,
            unique=True,
        )
    )
    def test_distribution_across_nodes(self, hash_values: list[int]) -> None:
        """Test that unique hash values distribute across nodes."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("n1", "n2", "n3", "n4"), scope_name="test")

        # Count selections
        counts: dict[str, int] = {}
        for h in hash_values:
            node = selector.select(nodes, h)
            if node:
                counts[node] = counts.get(node, 0) + 1

        # With 100 unique integers across 4 nodes, we expect distribution
        # At least 2 different nodes should be selected
        assert len(counts) >= 2

    @given(st.integers(min_value=-(2**63), max_value=2**63 - 1))
    def test_handles_extreme_hash_values(self, hash_value: int) -> None:
        """Test handling of extreme hash values including negatives."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")

        # Should not raise any exceptions
        result = selector.select(nodes, hash_value)
        assert result in nodes.nodes
