"""Tests for key route affinity module."""

import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock

from alternator.core.key_affinity import (
    AffinitySelector,
    PartitionKeyCache,
    extract_partition_key,
    get_table_name,
    is_rmw_operation,
    is_write_operation,
    should_use_affinity,
)
from alternator.core.live_nodes import NodeList


class TestIsRmwOperation:
    """Tests for is_rmw_operation function."""

    def test_update_with_condition_is_rmw(self) -> None:
        """Test UpdateItem with ConditionExpression is RMW."""
        params = {"ConditionExpression": "attribute_exists(pk)"}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_put_with_condition_is_rmw(self) -> None:
        """Test PutItem with ConditionExpression is RMW."""
        params = {"ConditionExpression": "attribute_not_exists(pk)"}
        assert is_rmw_operation("PutItem", params) is True

    def test_delete_with_condition_is_rmw(self) -> None:
        """Test DeleteItem with ConditionExpression is RMW."""
        params = {"ConditionExpression": "version = :v"}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_update_with_return_values_is_rmw(self) -> None:
        """Test UpdateItem with non-NONE ReturnValues is RMW."""
        params = {"ReturnValues": "ALL_OLD"}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_return_values_none_not_rmw(self) -> None:
        """Test UpdateItem with NONE ReturnValues is not RMW."""
        params = {"ReturnValues": "NONE"}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_update_without_condition_or_return_not_rmw(self) -> None:
        """Test UpdateItem without condition or return values is not RMW."""
        params = {"Key": {"pk": {"S": "123"}}}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_get_item_is_not_rmw(self) -> None:
        """Test GetItem is never RMW."""
        params = {"Key": {"pk": {"S": "123"}}}
        assert is_rmw_operation("GetItem", params) is False

    def test_scan_is_not_rmw(self) -> None:
        """Test Scan is never RMW."""
        params = {}
        assert is_rmw_operation("Scan", params) is False


class TestIsWriteOperation:
    """Tests for is_write_operation function."""

    def test_put_item_is_write(self) -> None:
        """Test PutItem is a write operation."""
        assert is_write_operation("PutItem") is True

    def test_update_item_is_write(self) -> None:
        """Test UpdateItem is a write operation."""
        assert is_write_operation("UpdateItem") is True

    def test_delete_item_is_write(self) -> None:
        """Test DeleteItem is a write operation."""
        assert is_write_operation("DeleteItem") is True

    def test_batch_write_is_write(self) -> None:
        """Test BatchWriteItem is a write operation."""
        assert is_write_operation("BatchWriteItem") is True

    def test_get_item_is_not_write(self) -> None:
        """Test GetItem is not a write operation."""
        assert is_write_operation("GetItem") is False

    def test_query_is_not_write(self) -> None:
        """Test Query is not a write operation."""
        assert is_write_operation("Query") is False

    def test_scan_is_not_write(self) -> None:
        """Test Scan is not a write operation."""
        assert is_write_operation("Scan") is False


class TestShouldUseAffinity:
    """Tests for should_use_affinity function."""

    def test_none_mode_always_false(self) -> None:
        """Test NONE mode always returns False."""
        params = {"ConditionExpression": "exists"}
        assert should_use_affinity("NONE", "PutItem", params) is False
        assert should_use_affinity("NONE", "UpdateItem", params) is False

    def test_rmw_mode_with_rmw_operation(self) -> None:
        """Test RMW mode with RMW operation."""
        params = {"ConditionExpression": "exists"}
        assert should_use_affinity("RMW", "PutItem", params) is True

    def test_rmw_mode_with_non_rmw_operation(self) -> None:
        """Test RMW mode with non-RMW operation."""
        params = {}
        assert should_use_affinity("RMW", "PutItem", params) is False

    def test_any_write_mode_with_write(self) -> None:
        """Test ANY_WRITE mode with write operation."""
        params = {}
        assert should_use_affinity("ANY_WRITE", "PutItem", params) is True
        assert should_use_affinity("ANY_WRITE", "UpdateItem", params) is True
        assert should_use_affinity("ANY_WRITE", "DeleteItem", params) is True

    def test_any_write_mode_with_read(self) -> None:
        """Test ANY_WRITE mode with read operation."""
        params = {}
        assert should_use_affinity("ANY_WRITE", "GetItem", params) is False
        assert should_use_affinity("ANY_WRITE", "Query", params) is False


class TestExtractPartitionKey:
    """Tests for extract_partition_key function."""

    def test_extract_from_key_string(self) -> None:
        """Test extracting string PK from Key."""
        params = {"Key": {"user_id": {"S": "user123"}}}
        result = extract_partition_key(params, "user_id")
        assert result == ("S", "user123")

    def test_extract_from_key_number(self) -> None:
        """Test extracting number PK from Key."""
        params = {"Key": {"id": {"N": "42"}}}
        result = extract_partition_key(params, "id")
        assert result == ("N", "42")

    def test_extract_from_key_binary(self) -> None:
        """Test extracting binary PK from Key."""
        binary_data = b"\x01\x02\x03"
        params = {"Key": {"data_id": {"B": binary_data}}}
        result = extract_partition_key(params, "data_id")
        assert result == ("B", binary_data)

    def test_extract_from_item(self) -> None:
        """Test extracting PK from Item (PutItem)."""
        params = {"Item": {"pk": {"S": "partition_key_value"}}}
        result = extract_partition_key(params, "pk")
        assert result == ("S", "partition_key_value")

    def test_key_not_found(self) -> None:
        """Test when partition key is not in params."""
        params = {"Key": {"other_key": {"S": "value"}}}
        result = extract_partition_key(params, "pk")
        assert result is None

    def test_empty_params(self) -> None:
        """Test with empty params."""
        result = extract_partition_key({}, "pk")
        assert result is None

    def test_extract_hash_key_from_composite_key_params(self) -> None:
        """Test extracting HASH key when both HASH and RANGE keys are present."""
        params = {
            "Key": {
                "pk": {"S": "partition_value"},
                "sk": {"S": "sort_value"},
            }
        }
        result = extract_partition_key(params, "pk")
        assert result == ("S", "partition_value")

    def test_extract_hash_key_from_composite_item(self) -> None:
        """Test extracting HASH key from Item with both HASH and RANGE keys."""
        params = {
            "Item": {
                "pk": {"S": "partition_value"},
                "sk": {"S": "sort_value"},
                "data": {"S": "extra"},
            }
        }
        result = extract_partition_key(params, "pk")
        assert result == ("S", "partition_value")

    def test_extract_ignores_range_key(self) -> None:
        """Test that extracting by HASH key name ignores RANGE key."""
        params = {
            "Key": {
                "user_id": {"S": "user_123"},
                "timestamp": {"N": "1704067200"},
            }
        }
        # Extract by HASH key name, RANGE key should be ignored
        result = extract_partition_key(params, "user_id")
        assert result == ("S", "user_123")

        # If we ask for the RANGE key by name, we can get it too
        result = extract_partition_key(params, "timestamp")
        assert result == ("N", "1704067200")


class TestGetTableName:
    """Tests for get_table_name function."""

    def test_table_name_present(self) -> None:
        """Test extracting present table name."""
        params = {"TableName": "users"}
        assert get_table_name(params) == "users"

    def test_table_name_missing(self) -> None:
        """Test when table name is missing."""
        params = {"Key": {"pk": {"S": "123"}}}
        assert get_table_name(params) is None


class TestAffinitySelector:
    """Tests for AffinitySelector class."""

    def test_select_from_empty_nodes(self) -> None:
        """Test selecting from empty node list."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=(), scope_name="test")
        assert selector.select(nodes, 12345) is None

    def test_select_deterministic(self) -> None:
        """Test selection is deterministic for same hash."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c", "d", "e"), scope_name="test")

        # Same hash should always select same node
        for _ in range(10):
            assert selector.select(nodes, 12345) == selector.select(nodes, 12345)

    def test_select_different_hashes_may_differ(self) -> None:
        """Test different hashes may select different nodes."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")

        # Different hashes should (statistically) select different nodes
        selections = {selector.select(nodes, h) for h in range(100)}
        assert len(selections) > 1  # Should have selected multiple different nodes

    def test_select_handles_negative_hash(self) -> None:
        """Test selection handles negative hash values."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")

        # Negative hash should still work
        result = selector.select(nodes, -12345)
        assert result in nodes.nodes

    def test_select_distribution(self) -> None:
        """Test hash-based selection distributes across nodes."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c", "d"), scope_name="test")

        # Count selections for different hash values
        counts: dict[str, int] = {}
        for h in range(1000):
            node = selector.select(nodes, h)
            if node:
                counts[node] = counts.get(node, 0) + 1

        # Each node should get roughly 250 selections (allow variance)
        for count in counts.values():
            assert 200 < count < 300


class TestPartitionKeyCache:
    """Tests for PartitionKeyCache class."""

    def test_cache_miss_calls_describe_table(self) -> None:
        """Test cache miss triggers DescribeTable call."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)
        result = cache.get_pk_name("test_table")

        assert result == "pk"
        mock_client.describe_table.assert_called_once_with(TableName="test_table")

    def test_cache_hit_skips_describe_table(self) -> None:
        """Test cache hit skips DescribeTable call."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)

        # First call - cache miss
        cache.get_pk_name("test_table")
        # Second call - cache hit
        cache.get_pk_name("test_table")

        # Should only call describe_table once
        assert mock_client.describe_table.call_count == 1

    def test_preload_populates_cache(self) -> None:
        """Test preload populates cache without API calls."""
        mock_client = MagicMock()
        cache = PartitionKeyCache(mock_client)

        cache.preload({"users": "user_id", "orders": "order_id"})

        # Should not call describe_table
        assert cache.get_pk_name("users") == "user_id"
        assert cache.get_pk_name("orders") == "order_id"
        mock_client.describe_table.assert_not_called()

    def test_clear_removes_cached_entries(self) -> None:
        """Test clear removes cached entries."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)
        cache.preload({"test_table": "pk"})

        cache.clear()

        # Should now call describe_table again
        cache.get_pk_name("test_table")
        mock_client.describe_table.assert_called_once()

    def test_handles_describe_table_error(self) -> None:
        """Test graceful handling of DescribeTable errors."""
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = Exception("Access denied")

        cache = PartitionKeyCache(mock_client)
        result = cache.get_pk_name("test_table")

        assert result is None

    def test_handles_missing_key_schema(self) -> None:
        """Test handling when KeySchema is missing."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {"Table": {}}

        cache = PartitionKeyCache(mock_client)
        result = cache.get_pk_name("test_table")

        assert result is None

    def test_handles_no_hash_key(self) -> None:
        """Test handling when no HASH key in schema."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)
        result = cache.get_pk_name("test_table")

        assert result is None

    def test_composite_key_returns_hash_key_only(self) -> None:
        """Test that only the HASH key is returned for composite key tables."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)
        result = cache.get_pk_name("composite_table")

        assert result == "pk"


class TestPartitionKeyCacheThreadSafety:
    """Stress tests for PartitionKeyCache thread safety."""

    def test_concurrent_get_pk_name_same_table(self) -> None:
        """Test concurrent access to same table."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                ]
            }
        }

        cache = PartitionKeyCache(mock_client)
        errors: list[Exception] = []
        results: list[str | None] = []
        lock = threading.Lock()

        def get_pk() -> None:
            try:
                for _ in range(50):
                    result = cache.get_pk_name("test_table")
                    with lock:
                        results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=get_pk) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 500
        assert all(r == "pk" for r in results)
        # Despite 500 calls, should only call describe_table once
        assert mock_client.describe_table.call_count == 1

    def test_concurrent_get_pk_name_different_tables(self) -> None:
        """Test concurrent access to different tables."""
        mock_client = MagicMock()

        def describe_table_side_effect(TableName: str) -> dict[str, object]:
            return {
                "Table": {
                    "KeySchema": [
                        {"AttributeName": f"pk_{TableName}", "KeyType": "HASH"},
                    ]
                }
            }

        mock_client.describe_table.side_effect = describe_table_side_effect

        cache = PartitionKeyCache(mock_client)
        errors: list[Exception] = []
        results: list[tuple[str, str | None]] = []
        lock = threading.Lock()

        def get_pk(table_id: int) -> None:
            try:
                table_name = f"table_{table_id}"
                for _ in range(20):
                    result = cache.get_pk_name(table_name)
                    with lock:
                        results.append((table_name, result))
            except Exception as e:
                errors.append(e)

        # 10 threads, each accessing a different table
        threads = [threading.Thread(target=get_pk, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(errors) == 0
        assert len(results) == 200

        # Verify each table got correct pk
        for table_name, pk in results:
            assert pk == f"pk_{table_name}"


class TestAffinitySelectorConcurrency:
    """Stress tests for AffinitySelector."""

    def test_high_concurrency_determinism(self) -> None:
        """Test that selection remains deterministic under high concurrency."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c", "d", "e"), scope_name="test")

        # Pre-compute expected results
        expected = {h: selector.select(nodes, h) for h in range(100)}

        with ThreadPoolExecutor(max_workers=50) as executor:
            for h in range(100):
                # Submit 100 selections for the same hash
                futures = [
                    executor.submit(selector.select, nodes, h) for _ in range(100)
                ]
                results = [f.result() for f in futures]
                # All results should match expected
                assert all(r == expected[h] for r in results)

    def test_concurrent_selection_distribution(self) -> None:
        """Test hash distribution under high concurrency."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("n1", "n2", "n3", "n4"), scope_name="test")

        # Use different hash values
        hashes = list(range(10000))

        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(selector.select, nodes, h) for h in hashes]
            results = [f.result() for f in futures]

        counter: Counter[str | None] = Counter(results)

        # Distribution should be roughly even
        for node in ("n1", "n2", "n3", "n4"):
            assert 2000 < counter[node] < 3000
