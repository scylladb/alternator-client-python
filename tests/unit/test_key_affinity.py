"""Tests for key route affinity module."""

import contextlib
import copy
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock

from botocore.awsrequest import AWSPreparedRequest

from alternator.config import Config
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.hashing import hash_attribute_value
from alternator.core.key_affinity import (
    AffinitySelector,
    PartitionKeyCache,
    extract_partition_key,
    get_table_name,
    is_rmw_operation,
    is_write_operation,
    select_affinity_node,
    should_use_affinity,
)
from alternator.core.live_nodes import NodeList
from alternator.core.request import extract_request_params


def _batch_write_routing_target(
    params: dict[str, Any],
) -> tuple[str | None, tuple[str, Any] | None, int | None]:
    table_name = get_table_name(params)
    pk = extract_partition_key(params, "pk")
    if pk is None:
        return (table_name, None, None)

    attr_type, value = pk
    return (table_name, pk, hash_attribute_value(attr_type, value))


def _pk_value_for_node(nodes: NodeList, target_node: str, prefix: str) -> str:
    selector = AffinitySelector()
    for index in range(1000):
        value = f"{prefix}-{index}"
        hash_value = hash_attribute_value("S", value)
        if selector.select(nodes, hash_value) == target_node:
            return value
    raise AssertionError(f"could not find value for node {target_node}")


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

    def test_put_with_empty_condition_is_not_rmw(self) -> None:
        """Test PutItem with empty ConditionExpression is not RMW."""
        params = {"ConditionExpression": ""}
        assert is_rmw_operation("PutItem", params) is False

    def test_put_with_expected_is_rmw(self) -> None:
        """Test PutItem with Expected is RMW."""
        params = {"Expected": {}}
        assert is_rmw_operation("PutItem", params) is True

    def test_put_with_all_old_return_values_is_rmw(self) -> None:
        """Test PutItem with ALL_OLD ReturnValues is RMW."""
        params = {"ReturnValues": "ALL_OLD"}
        assert is_rmw_operation("PutItem", params) is True

    def test_put_with_none_return_values_is_not_rmw(self) -> None:
        """Test PutItem with NONE ReturnValues is not RMW."""
        params = {"ReturnValues": "NONE"}
        assert is_rmw_operation("PutItem", params) is False

    def test_delete_with_condition_is_rmw(self) -> None:
        """Test DeleteItem with ConditionExpression is RMW."""
        params = {"ConditionExpression": "version = :v"}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_empty_condition_is_not_rmw(self) -> None:
        """Test DeleteItem with empty ConditionExpression is not RMW."""
        params = {"ConditionExpression": ""}
        assert is_rmw_operation("DeleteItem", params) is False

    def test_delete_with_expected_is_rmw(self) -> None:
        """Test DeleteItem with Expected is RMW."""
        params = {"Expected": {}}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_all_old_return_values_is_rmw(self) -> None:
        """Test DeleteItem with ALL_OLD ReturnValues is RMW."""
        params = {"ReturnValues": "ALL_OLD"}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_updated_new_return_values_is_not_rmw(self) -> None:
        """Test DeleteItem only treats ALL_OLD ReturnValues as RMW."""
        params = {"ReturnValues": "UPDATED_NEW"}
        assert is_rmw_operation("DeleteItem", params) is False

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

    def test_update_with_update_expression_is_rmw(self) -> None:
        """Test UpdateItem with UpdateExpression is RMW."""
        params = {"UpdateExpression": "SET value = :v"}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_empty_update_expression_is_not_rmw(self) -> None:
        """Test empty UpdateExpression alone is not RMW."""
        params = {"UpdateExpression": ""}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_update_with_expected_is_rmw(self) -> None:
        """Test UpdateItem with Expected is RMW."""
        params = {"Expected": {}}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_empty_return_values_is_not_rmw(self) -> None:
        """Test UpdateItem with empty ReturnValues is not RMW."""
        params = {"ReturnValues": ""}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_update_with_updated_new_return_values_is_not_rmw(self) -> None:
        """Test UpdateItem UPDATED_NEW ReturnValues is not RMW."""
        params = {"ReturnValues": "UPDATED_NEW"}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_update_with_all_new_return_values_is_rmw(self) -> None:
        """Test UpdateItem ReturnValues other than allowed no-read values is RMW."""
        params = {"ReturnValues": "ALL_NEW"}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_attribute_updates_add_is_rmw(self) -> None:
        """Test AttributeUpdates ADD action is RMW."""
        params = {"AttributeUpdates": {"count": {"Action": "ADD", "Value": {"N": "1"}}}}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_attribute_updates_delete_with_value_is_rmw(self) -> None:
        """Test AttributeUpdates DELETE action with a value is RMW."""
        params = {
            "AttributeUpdates": {
                "tags": {"Action": "DELETE", "Value": {"SS": ["old"]}},
            }
        }
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_attribute_updates_delete_without_value_not_rmw(self) -> None:
        """Test AttributeUpdates DELETE action without a value is not RMW."""
        params = {"AttributeUpdates": {"tags": {"Action": "DELETE"}}}
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

    def test_rmw_mode_with_batch_write(self) -> None:
        """Test RMW mode does not use affinity for BatchWriteItem."""
        params = {
            "RequestItems": {
                "users": [
                    {
                        "PutRequest": {
                            "Item": {
                                "user_id": {"S": "user123"},
                            }
                        }
                    }
                ]
            }
        }
        assert should_use_affinity("RMW", "BatchWriteItem", params) is False

    def test_any_write_mode_with_write(self) -> None:
        """Test ANY_WRITE mode with write operation."""
        params = {}
        assert should_use_affinity("ANY_WRITE", "PutItem", params) is True
        assert should_use_affinity("ANY_WRITE", "UpdateItem", params) is True
        assert should_use_affinity("ANY_WRITE", "DeleteItem", params) is True
        assert should_use_affinity("ANY_WRITE", "BatchWriteItem", params) is True

    def test_any_write_mode_with_read(self) -> None:
        """Test ANY_WRITE mode with read operation."""
        params = {}
        assert should_use_affinity("ANY_WRITE", "GetItem", params) is False
        assert should_use_affinity("ANY_WRITE", "Query", params) is False


class TestSelectAffinityNode:
    """Tests for preferred node selection."""

    def test_single_put_item_selects_node(self) -> None:
        """Test single PutItem routes by the item partition key."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "b", "put")
        params = {
            "TableName": "orders",
            "Item": {"pk": {"S": value}},
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="PutItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == "b"
        )

    def test_single_delete_item_selects_node(self) -> None:
        """Test single DeleteItem routes by the key partition key."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "c", "delete")
        params = {
            "TableName": "orders",
            "Key": {"pk": {"S": value}},
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="DeleteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == "c"
        )

    def test_single_put_item_binary_pk_decodes_prepared_json(self) -> None:
        """Real botocore JSON base64 binary values are decoded before hashing."""
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config as BotoConfig

        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        binary_value = b"\x00\x01stable"
        expected = AffinitySelector().select(
            nodes,
            hash_attribute_value("B", binary_value),
        )
        captured_params: dict[str, Any] = {}

        client = boto3.client(
            "dynamodb",
            endpoint_url="http://localhost:1",
            region_name="us-east-1",
            config=BotoConfig(signature_version=UNSIGNED),
        )

        def capture_request(request: AWSPreparedRequest, **_: object) -> None:
            captured_params.update(extract_request_params(request))
            raise RuntimeError("captured")

        client.meta.events.register_last("before-send.dynamodb.PutItem", capture_request)

        with contextlib.suppress(RuntimeError):
            client.put_item(
                TableName="orders",
                Item={"pk": {"B": binary_value}},
            )

        assert captured_params["Item"]["pk"]["B"] == "AAFzdGFibGU="
        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="PutItem",
                params=captured_params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == expected
        )

    def test_batch_write_single_put_selects_node(self) -> None:
        """Test BatchWriteItem with a single PutRequest selects its node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "a", "batch-put")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": value}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == "a"
        )

    def test_batch_write_single_delete_selects_node(self) -> None:
        """Test BatchWriteItem with a single DeleteRequest selects its node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "c", "batch-delete")
        params = {
            "RequestItems": {
                "orders": [{"DeleteRequest": {"Key": {"pk": {"S": value}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == "c"
        )

    def test_batch_write_mixed_put_delete_unique_winner(self) -> None:
        """Test BatchWriteItem votes for the unique preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_node(nodes, "b", "b1")
        b2 = _pk_value_for_node(nodes, "b", "b2")
        c1 = _pk_value_for_node(nodes, "c", "c1")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": b1}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": b2}}}},
                    {"PutRequest": {"Item": {"pk": {"S": c1}}}},
                ],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == "b"
        )

    def test_batch_write_multi_table_reversed_order_same_winner(self) -> None:
        """Test batch voting is independent of table and request order."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_node(nodes, "b", "orders-b1")
        b2 = _pk_value_for_node(nodes, "b", "sessions-b2")
        a1 = _pk_value_for_node(nodes, "a", "orders-a1")
        orders = [
            {"PutRequest": {"Item": {"pk": {"S": b1}}}},
            {"PutRequest": {"Item": {"pk": {"S": a1}}}},
        ]
        sessions = [{"DeleteRequest": {"Key": {"pk": {"S": b2}}}}]
        params_a = {"RequestItems": {"orders": orders, "sessions": sessions}}
        params_b = {
            "RequestItems": {
                "sessions": list(reversed(sessions)),
                "orders": list(reversed(orders)),
            }
        }

        for params in (params_a, params_b):
            assert (
                select_affinity_node(
                    mode="ANY_WRITE",
                    operation_name="BatchWriteItem",
                    params=params,
                    nodes=nodes,
                    get_pk_name={"orders": "pk", "sessions": "pk"}.get,
                )
                == "b"
            )

    def test_batch_write_missing_pk_metadata_falls_back(self) -> None:
        """Test missing partition-key metadata produces no preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": "value"}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={}.get,
            )
            is None
        )

    def test_batch_write_missing_pk_value_falls_back(self) -> None:
        """Test missing partition-key value produces no preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"other": {"S": "value"}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            is None
        )

    def test_batch_write_unsupported_pk_type_falls_back(self) -> None:
        """Test unsupported partition-key type produces no preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"BOOL": True}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            is None
        )

    def test_batch_write_no_nodes_falls_back(self) -> None:
        """Test no active nodes produces no preferred node."""
        nodes = NodeList(nodes=(), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": "value"}}}}],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            is None
        )

    def test_batch_write_tied_votes_fall_back(self) -> None:
        """Test tied preferred-node votes produce no preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        a1 = _pk_value_for_node(nodes, "a", "tie-a")
        b1 = _pk_value_for_node(nodes, "b", "tie-b")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": a1}}}},
                    {"PutRequest": {"Item": {"pk": {"S": b1}}}},
                ],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            is None
        )

    def test_batch_write_binary_pk_selects_stable_node(self) -> None:
        """Test binary partition-key values use stable hashing."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        binary_value = b"\x00\x01stable"
        expected = AffinitySelector().select(
            nodes,
            hash_attribute_value("B", binary_value),
        )
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"B": binary_value}}}},
                ],
            }
        }

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            )
            == expected
        )

    def test_batch_write_selection_does_not_mutate_params(self) -> None:
        """Test BatchWriteItem affinity selection leaves request params unchanged."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "b", "no-mutate")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": value}}}}],
            }
        }
        original = copy.deepcopy(params)

        select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        )

        assert params == original


class TestAffinityHandlerRouting:
    """Tests for preferred-node routing through shared request handlers."""

    def test_preferred_node_first_and_remaining_nodes_preserved(self) -> None:
        """Test handler tries preferred node first without dropping retries."""
        config = Config(seed_hosts=["seed"], port=8000)
        manager = MagicMock()
        manager.nodes = NodeList(nodes=("a", "b", "c"), scope_name="cluster")
        events = MagicMock()

        def compute_affinity_node(
            operation_name: str,
            params: dict[str, Any],
            nodes: NodeList,
        ) -> str | None:
            assert operation_name == "PutItem"
            assert params == {"TableName": "orders"}
            assert nodes.nodes == ("a", "b", "c")
            return "b"

        _register_alternator_handlers(
            events,
            manager,
            config,
            compute_affinity_node,
        )
        handlers = {
            call[0][1].__name__: call[0][1] for call in events.register.call_args_list
        }

        request = MagicMock()
        request.url = "http://seed:8000/"
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.PutItem"}
        request.body = b'{"TableName": "orders"}'
        request._alternator_query_plan = None

        update_endpoint = handlers["update_endpoint"]
        update_endpoint(request)
        first_url = request.url
        update_endpoint(request)
        second_url = request.url
        update_endpoint(request)
        third_url = request.url

        assert first_url == "http://b:8000/"
        assert {second_url, third_url} == {"http://a:8000/", "http://c:8000/"}


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

    def test_extract_from_batch_write_put_request(self) -> None:
        """Test extracting PK from BatchWriteItem PutRequest."""
        params = {
            "RequestItems": {
                "orders": [
                    {
                        "PutRequest": {
                            "Item": {
                                "order_id": {"S": "order123"},
                                "data": {"S": "value"},
                            }
                        }
                    }
                ]
            }
        }
        result = extract_partition_key(params, "order_id")
        assert result == ("S", "order123")

    def test_extract_from_batch_write_delete_request(self) -> None:
        """Test extracting PK from BatchWriteItem DeleteRequest."""
        params = {
            "RequestItems": {
                "sessions": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "session_id": {"S": "session123"},
                            }
                        }
                    }
                ]
            }
        }
        result = extract_partition_key(params, "session_id")
        assert result == ("S", "session123")

    def test_extract_from_batch_write_is_table_order_independent(self) -> None:
        """Test batch affinity target is independent of RequestItems order."""
        orders = [
            {
                "PutRequest": {
                    "Item": {
                        "pk": {"S": "order123"},
                    }
                }
            }
        ]
        sessions = [
            {
                "DeleteRequest": {
                    "Key": {
                        "pk": {"S": "session123"},
                    }
                }
            }
        ]
        params_a = {"RequestItems": {"orders": orders, "sessions": sessions}}
        params_b = {"RequestItems": {"sessions": sessions, "orders": orders}}

        assert get_table_name(params_a) == "orders"
        assert get_table_name(params_b) == "orders"
        assert extract_partition_key(params_a, "pk") == ("S", "order123")
        assert extract_partition_key(params_b, "pk") == ("S", "order123")

    def test_extract_from_batch_write_is_write_order_independent(self) -> None:
        """Test batch affinity target is independent of write order."""
        write_a = {
            "PutRequest": {
                "Item": {
                    "pk": {"S": "order123"},
                }
            }
        }
        write_b = {
            "PutRequest": {
                "Item": {
                    "pk": {"S": "order456"},
                }
            }
        }
        params_a = {"RequestItems": {"orders": [write_a, write_b]}}
        params_b = {"RequestItems": {"orders": [write_b, write_a]}}

        assert extract_partition_key(params_a, "pk") == ("S", "order123")
        assert extract_partition_key(params_b, "pk") == ("S", "order123")

    def test_batch_write_routing_hash_is_deterministic_for_same_request(self) -> None:
        """Test equivalent BatchWriteItem requests use the same routing hash."""
        params_a = {
            "RequestItems": {
                "sessions": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "pk": {"S": "session123"},
                            }
                        }
                    }
                ],
                "orders": [
                    {
                        "PutRequest": {
                            "Item": {
                                "data": {"S": "value"},
                                "pk": {"S": "order123"},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": "order456"},
                                "data": {"S": "value"},
                            }
                        }
                    },
                ],
            }
        }
        params_b = {
            "RequestItems": {
                "orders": [
                    {
                        "PutRequest": {
                            "Item": {
                                "data": {"S": "value"},
                                "pk": {"S": "order456"},
                            }
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {
                                "pk": {"S": "order123"},
                                "data": {"S": "value"},
                            }
                        }
                    },
                ],
                "sessions": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "pk": {"S": "session123"},
                            }
                        }
                    }
                ],
            }
        }

        assert _batch_write_routing_target(params_a) == _batch_write_routing_target(
            params_b
        )

    def test_batch_write_routing_hash_is_deterministic_for_repeated_build(
        self,
    ) -> None:
        """Test repeated same-way BatchWriteItem construction routes identically."""

        def build_request() -> dict[str, Any]:
            return {
                "RequestItems": {
                    "orders": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "pk": {"S": "order123"},
                                    "data": {"S": "value"},
                                }
                            }
                        }
                    ],
                    "sessions": [
                        {
                            "DeleteRequest": {
                                "Key": {
                                    "pk": {"S": "session123"},
                                }
                            }
                        }
                    ],
                }
            }

        targets = {_batch_write_routing_target(build_request()) for _ in range(10)}

        assert targets == {
            ("orders", ("S", "order123"), hash_attribute_value("S", "order123"))
        }

    def test_extract_from_empty_batch_write(self) -> None:
        """Test empty BatchWriteItem does not produce a PK."""
        params: dict[str, object] = {"RequestItems": {}}
        result = extract_partition_key(params, "pk")
        assert result is None

    def test_extract_from_batch_get_shape(self) -> None:
        """Test BatchGetItem RequestItems are not treated as batch writes."""
        params = {
            "RequestItems": {
                "users": {
                    "Keys": [
                        {
                            "user_id": {"S": "user123"},
                        }
                    ]
                }
            }
        }
        result = extract_partition_key(params, "user_id")
        assert result is None

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

    def test_table_name_from_batch_write_put_request(self) -> None:
        """Test extracting table name from BatchWriteItem PutRequest."""
        params = {
            "RequestItems": {
                "orders": [
                    {
                        "PutRequest": {
                            "Item": {
                                "order_id": {"S": "order123"},
                            }
                        }
                    }
                ]
            }
        }
        assert get_table_name(params) == "orders"

    def test_table_name_from_batch_write_delete_request(self) -> None:
        """Test extracting table name from BatchWriteItem DeleteRequest."""
        params = {
            "RequestItems": {
                "sessions": [
                    {
                        "DeleteRequest": {
                            "Key": {
                                "session_id": {"S": "session123"},
                            }
                        }
                    }
                ]
            }
        }
        assert get_table_name(params) == "sessions"

    def test_table_name_from_empty_batch_write(self) -> None:
        """Test empty BatchWriteItem does not produce a table name."""
        params: dict[str, object] = {"RequestItems": {}}
        assert get_table_name(params) is None

    def test_table_name_from_batch_get_shape(self) -> None:
        """Test BatchGetItem RequestItems are not treated as batch writes."""
        params = {
            "RequestItems": {
                "users": {
                    "Keys": [
                        {
                            "user_id": {"S": "user123"},
                        }
                    ]
                }
            }
        }
        assert get_table_name(params) is None

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
