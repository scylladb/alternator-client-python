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

"""Tests for key route affinity module."""

import contextlib
import copy
import gc
import threading
import time
import weakref
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest.mock import MagicMock, patch

from botocore.awsrequest import AWSPreparedRequest

from alternator.config import Config
from alternator.core.handlers import _register_alternator_handlers
from alternator.core.hashing import hash_attribute_value
from alternator.core.key_affinity import (
    AffinitySelector,
    PartitionKeyCache,
    SeededAffinityPlan,
    extract_partition_key,
    get_table_name,
    is_rmw_operation,
    is_write_operation,
    select_affinity_node,
    should_use_affinity,
)
from alternator.core.live_nodes import NodeList
from alternator.core.query_plan import LazyQueryPlan
from alternator.core.request import extract_request_params

Params = dict[str, Any]


def _wait_until(predicate: Callable[[], bool], timeout: float = 1.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return bool(predicate())


def _batch_write_routing_target(
    params: Params,
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


def _query_plan_first_node(nodes: NodeList, hash_value: int) -> str:
    return next(LazyQueryPlan(nodes=tuple(sorted(nodes.nodes)), seed=hash_value))


def _pk_value_for_batch_node(nodes: NodeList, target_node: str, prefix: str) -> str:
    for index in range(1000):
        value = f"{prefix}-{index}"
        hash_value = hash_attribute_value("S", value)
        if _query_plan_first_node(nodes, hash_value) == target_node:
            return value
    raise AssertionError(f"could not find batch value for node {target_node}")


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
        """Test PutItem with non-empty Expected is RMW."""
        params = {"Expected": {"pk": {"Exists": False}}}
        assert is_rmw_operation("PutItem", params) is True

    def test_put_with_empty_expected_is_not_rmw(self) -> None:
        """Test PutItem with empty Expected is not RMW."""
        params: Params = {"Expected": {}}
        assert is_rmw_operation("PutItem", params) is False

    def test_put_with_all_old_return_values_is_rmw(self) -> None:
        """Test PutItem with ALL_OLD ReturnValues is RMW."""
        params = {"ReturnValues": "ALL_OLD"}
        assert is_rmw_operation("PutItem", params) is True

    def test_put_with_none_return_values_is_not_rmw(self) -> None:
        """Test PutItem with NONE ReturnValues is not RMW."""
        params = {"ReturnValues": "NONE"}
        assert is_rmw_operation("PutItem", params) is False

    def test_put_with_non_none_return_values_is_rmw(self) -> None:
        """PutItem treats every non-NONE return mode as read-before-write."""
        params = {"ReturnValues": "UPDATED_NEW"}
        assert is_rmw_operation("PutItem", params) is True

    def test_delete_with_condition_is_rmw(self) -> None:
        """Test DeleteItem with ConditionExpression is RMW."""
        params = {"ConditionExpression": "version = :v"}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_empty_condition_is_not_rmw(self) -> None:
        """Test DeleteItem with empty ConditionExpression is not RMW."""
        params = {"ConditionExpression": ""}
        assert is_rmw_operation("DeleteItem", params) is False

    def test_delete_with_expected_is_rmw(self) -> None:
        """Test DeleteItem with non-empty Expected is RMW."""
        params = {"Expected": {"pk": {"Exists": True}}}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_empty_expected_is_not_rmw(self) -> None:
        """Test DeleteItem with empty Expected is not RMW."""
        params: Params = {"Expected": {}}
        assert is_rmw_operation("DeleteItem", params) is False

    def test_delete_with_all_old_return_values_is_rmw(self) -> None:
        """Test DeleteItem with ALL_OLD ReturnValues is RMW."""
        params = {"ReturnValues": "ALL_OLD"}
        assert is_rmw_operation("DeleteItem", params) is True

    def test_delete_with_updated_new_return_values_is_rmw(self) -> None:
        """DeleteItem treats every non-NONE return mode as read-before-write."""
        params = {"ReturnValues": "UPDATED_NEW"}
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

    def test_update_with_update_expression_is_rmw(self) -> None:
        """Test UpdateItem with UpdateExpression is RMW."""
        params = {"UpdateExpression": "SET value = :v"}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_empty_update_expression_is_not_rmw(self) -> None:
        """Test empty UpdateExpression alone is not RMW."""
        params = {"UpdateExpression": ""}
        assert is_rmw_operation("UpdateItem", params) is False

    def test_update_with_expected_is_rmw(self) -> None:
        """Test UpdateItem with non-empty Expected is RMW."""
        params = {"Expected": {"pk": {"Exists": True}}}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_update_with_empty_expected_is_not_rmw(self) -> None:
        """Test UpdateItem with empty Expected is not RMW."""
        params: Params = {"Expected": {}}
        assert is_rmw_operation("UpdateItem", params) is False

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

    def test_update_with_unknown_return_values_is_not_rmw(self) -> None:
        """Unknown return modes do not create an affinity plan."""
        params = {"ReturnValues": "FUTURE_MODE"}
        assert is_rmw_operation("UpdateItem", params) is False

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

    def test_update_with_attribute_updates_delete_empty_value_is_rmw(self) -> None:
        """Legacy DELETE qualifies whenever its Value member is present."""
        params = {"AttributeUpdates": {"tags": {"Action": "DELETE", "Value": {}}}}
        assert is_rmw_operation("UpdateItem", params) is True

    def test_get_item_is_not_rmw(self) -> None:
        """Test GetItem is never RMW."""
        params = {"Key": {"pk": {"S": "123"}}}
        assert is_rmw_operation("GetItem", params) is False

    def test_scan_is_not_rmw(self) -> None:
        """Test Scan is never RMW."""
        params: Params = {}
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
        params: Params = {}
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
        params: Params = {}
        assert should_use_affinity("ANY_WRITE", "PutItem", params) is True
        assert should_use_affinity("ANY_WRITE", "UpdateItem", params) is True
        assert should_use_affinity("ANY_WRITE", "DeleteItem", params) is True
        batch_params = {
            "RequestItems": {
                "users": [{"DeleteRequest": {"Key": {"pk": {"S": "value"}}}}]
            }
        }
        assert should_use_affinity("ANY_WRITE", "BatchWriteItem", batch_params) is True

    def test_any_write_mode_with_read(self) -> None:
        """Test ANY_WRITE mode with read operation."""
        params: Params = {}
        assert should_use_affinity("ANY_WRITE", "GetItem", params) is False
        assert should_use_affinity("ANY_WRITE", "Query", params) is False

    def test_any_write_empty_batch_is_not_affinity_eligible(self) -> None:
        """Batch affinity requires at least one usable put or delete."""
        assert (
            should_use_affinity("ANY_WRITE", "BatchWriteItem", {"RequestItems": {}})
            is False
        )


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

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="PutItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == SeededAffinityPlan(hash_attribute_value("S", value))

    def test_single_delete_item_selects_node(self) -> None:
        """Test single DeleteItem routes by the key partition key."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_node(nodes, "c", "delete")
        params = {
            "TableName": "orders",
            "Key": {"pk": {"S": value}},
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="DeleteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == SeededAffinityPlan(hash_attribute_value("S", value))

    def test_single_put_item_binary_pk_decodes_prepared_json(self) -> None:
        """Real botocore JSON base64 binary values are decoded before hashing."""
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config as BotoConfig

        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        binary_value = b"\x00\x01stable"
        expected = SeededAffinityPlan(hash_attribute_value("B", binary_value))
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

        client.meta.events.register_last(
            "before-send.dynamodb.PutItem", capture_request
        )

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
        value = _pk_value_for_batch_node(nodes, "a", "batch-put")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": value}}}}],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("a",)

    def test_batch_write_vote_uses_query_plan_first_pick(self) -> None:
        """Test BatchWriteItem votes use canonical query-plan first pick."""
        nodes = NodeList(
            nodes=("node1", "node2", "node3", "node4", "node5", "node6"),
            scope_name="test",
        )
        value = None
        expected = None
        modulo = None
        for index in range(1000):
            candidate = f"canonical-batch-{index}"
            hash_value = hash_attribute_value("S", candidate)
            query_plan_node = _query_plan_first_node(nodes, hash_value)
            sorted_nodes = tuple(sorted(nodes.nodes))
            modulo_node = sorted_nodes[abs(hash_value) % len(sorted_nodes)]
            if query_plan_node != modulo_node:
                value = candidate
                expected = query_plan_node
                modulo = modulo_node
                break

        assert value is not None
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": value}}}}],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == (expected,)
        assert expected != modulo

    def test_batch_write_single_delete_selects_node(self) -> None:
        """Test BatchWriteItem with a single DeleteRequest selects its node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_batch_node(nodes, "c", "batch-delete")
        params = {
            "RequestItems": {
                "orders": [{"DeleteRequest": {"Key": {"pk": {"S": value}}}}],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("c",)

    def test_batch_write_mixed_put_delete_unique_winner(self) -> None:
        """Test BatchWriteItem votes for the unique preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_batch_node(nodes, "b", "b1")
        b2 = _pk_value_for_batch_node(nodes, "b", "b2")
        c1 = _pk_value_for_batch_node(nodes, "c", "c1")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": b1}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": b2}}}},
                    {"PutRequest": {"Item": {"pk": {"S": c1}}}},
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("b", "c")

    def test_batch_write_multi_table_reversed_order_same_winner(self) -> None:
        """Test batch voting is independent of table and request order."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_batch_node(nodes, "b", "orders-b1")
        b2 = _pk_value_for_batch_node(nodes, "b", "sessions-b2")
        a1 = _pk_value_for_batch_node(nodes, "a", "orders-a1")
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
            assert select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk", "sessions": "pk"}.get,
            ) == ("b", "a")

    def test_batch_write_missing_pk_metadata_falls_back(self) -> None:
        """Test missing partition-key metadata produces no preferred node."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [{"PutRequest": {"Item": {"pk": {"S": "value"}}}}],
            }
        }

        def no_pk_name(table_name: str) -> str | None:
            return None

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name=no_pk_name,
            )
            is None
        )

    def test_batch_write_snapshots_missing_metadata_per_table(self) -> None:
        """A lookup completing mid-request cannot route only part of a batch."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": "first"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "second"}}}},
                ],
            }
        }
        lookups: list[str] = []

        def metadata_becomes_available(table_name: str) -> str | None:
            lookups.append(table_name)
            return None if len(lookups) == 1 else "pk"

        assert (
            select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name=metadata_becomes_available,
            )
            is None
        )
        assert lookups == ["orders"]

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

    def test_batch_write_tied_votes_use_node_address_tie_break(self) -> None:
        """Test tied preferred-node votes use node address order."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        a1 = _pk_value_for_batch_node(nodes, "a", "tie-a")
        b1 = _pk_value_for_batch_node(nodes, "b", "tie-b")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": a1}}}},
                    {"PutRequest": {"Item": {"pk": {"S": b1}}}},
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("a", "b")

    def test_batch_write_unknown_table_metadata_does_not_block_known_table(
        self,
    ) -> None:
        """Unknown table metadata is skipped while another table can vote."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_batch_node(nodes, "b", "known-table")
        params = {
            "RequestItems": {
                "unknown": [
                    {"PutRequest": {"Item": {"pk": {"S": "ignored"}}}},
                ],
                "orders": [
                    {"DeleteRequest": {"Key": {"pk": {"S": b1}}}},
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("b",)

    def test_batch_write_unusable_candidates_are_skipped(self) -> None:
        """Missing and unsupported keys are skipped while valid candidates vote."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        c1 = _pk_value_for_batch_node(nodes, "c", "usable")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"BOOL": True}}}},
                    {"DeleteRequest": {"Key": {"other": {"S": "missing-pk"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": c1}}}},
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("c",)

    def test_batch_write_non_key_attributes_do_not_change_winner(self) -> None:
        """Non-key payload attributes do not affect batch affinity voting."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        b1 = _pk_value_for_batch_node(nodes, "b", "payload-b1")
        b2 = _pk_value_for_batch_node(nodes, "b", "payload-b2")
        a1 = _pk_value_for_batch_node(nodes, "a", "payload-a1")
        params_a = {
            "RequestItems": {
                "orders": [
                    {
                        "PutRequest": {
                            "Item": {"pk": {"S": b1}, "note": {"S": "before"}}
                        }
                    },
                    {
                        "PutRequest": {
                            "Item": {"pk": {"S": b2}, "note": {"S": "before"}}
                        }
                    },
                    {
                        "DeleteRequest": {
                            "Key": {"pk": {"S": a1}, "note": {"S": "before"}}
                        }
                    },
                ],
            }
        }
        params_b = {
            "RequestItems": {
                "orders": [
                    {
                        "DeleteRequest": {
                            "Key": {"pk": {"S": a1}, "note": {"S": "after"}}
                        }
                    },
                    {"PutRequest": {"Item": {"pk": {"S": b2}, "note": {"S": "after"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": b1}, "note": {"S": "after"}}}},
                ],
            }
        }

        for params in (params_a, params_b):
            assert select_affinity_node(
                mode="ANY_WRITE",
                operation_name="BatchWriteItem",
                params=params,
                nodes=nodes,
                get_pk_name={"orders": "pk"}.get,
            ) == ("b", "a")

    def test_batch_write_invalid_union_write_request_is_ignored(self) -> None:
        """A malformed write containing put and delete does not contribute votes."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        a1 = _pk_value_for_batch_node(nodes, "a", "valid-write")
        b1 = _pk_value_for_batch_node(nodes, "b", "invalid-put")
        b2 = _pk_value_for_batch_node(nodes, "b", "invalid-delete")
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": a1}}}},
                    {
                        "PutRequest": {"Item": {"pk": {"S": b1}}},
                        "DeleteRequest": {"Key": {"pk": {"S": b2}}},
                    },
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == ("a",)

    def test_batch_write_binary_pk_selects_stable_node(self) -> None:
        """Test binary partition-key values use stable hashing."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        binary_value = b"\x00\x01stable"
        expected = _query_plan_first_node(
            nodes, hash_attribute_value("B", binary_value)
        )
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"B": binary_value}}}},
                ],
            }
        }

        assert select_affinity_node(
            mode="ANY_WRITE",
            operation_name="BatchWriteItem",
            params=params,
            nodes=nodes,
            get_pk_name={"orders": "pk"}.get,
        ) == (expected,)

    def test_batch_write_selection_does_not_mutate_params(self) -> None:
        """Test BatchWriteItem affinity selection leaves request params unchanged."""
        nodes = NodeList(nodes=("a", "b", "c"), scope_name="test")
        value = _pk_value_for_batch_node(nodes, "b", "no-mutate")
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

    def test_seeded_plan_and_retry_cycle_match_canonical_order(self) -> None:
        """Single-item affinity retries preserve the full canonical plan."""
        config = Config(seed_hosts=["seed"], port=8000)
        manager = MagicMock()
        manager.nodes = NodeList(nodes=("a", "b", "c"), scope_name="cluster")
        events = MagicMock()

        def compute_affinity_node(
            operation_name: str,
            params: dict[str, Any],
            nodes: NodeList,
        ) -> SeededAffinityPlan:
            assert operation_name == "PutItem"
            assert params == {"TableName": "orders"}
            assert nodes.nodes == ("a", "b", "c")
            return SeededAffinityPlan(seed=42)

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
        urls: list[str] = []
        for _ in range(6):
            update_endpoint(request)
            urls.append(request.url)

        expected_cycle = [
            f"http://{node}:8000/"
            for node in LazyQueryPlan(nodes=("a", "b", "c"), seed=42)
        ]
        assert urls == expected_cycle * 2

    def test_query_plan_brackets_ipv6_nodes(self) -> None:
        """Test request handler formats raw IPv6 node addresses as URL authorities."""
        config = Config(seed_hosts=["seed"], port=8000)
        manager = MagicMock()
        manager.nodes = NodeList(
            nodes=("2001:db8::1", "2001:db8::2"), scope_name="cluster"
        )
        events = MagicMock()

        _register_alternator_handlers(events, manager, config)
        handlers = {
            call[0][1].__name__: call[0][1] for call in events.register.call_args_list
        }

        request = MagicMock()
        request.url = "http://seed:8000/"
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.ListTables"}
        request.body = b"{}"
        request._alternator_query_plan = None

        update_endpoint = handlers["update_endpoint"]
        update_endpoint(request)

        assert request.url in {
            "http://[2001:db8::1]:8000/",
            "http://[2001:db8::2]:8000/",
        }

    def test_batch_write_preferred_node_first_and_remaining_nodes_preserved(
        self,
    ) -> None:
        """Test BatchWriteItem affinity keeps retries after the preferred node."""
        config = Config(seed_hosts=["seed"], port=8000)
        manager = MagicMock()
        manager.nodes = NodeList(nodes=("a", "b", "c"), scope_name="cluster")
        events = MagicMock()
        params = {
            "RequestItems": {
                "orders": [
                    {"PutRequest": {"Item": {"pk": {"S": "order-1"}}}},
                ],
            }
        }

        def compute_affinity_node(
            operation_name: str,
            request_params: dict[str, Any],
            nodes: NodeList,
        ) -> tuple[str, ...] | None:
            assert operation_name == "BatchWriteItem"
            assert request_params == params
            assert nodes.nodes == ("a", "b", "c")
            return ("c", "a")

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
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.BatchWriteItem"}
        request.body = b'{"RequestItems":{"orders":[{"PutRequest":{"Item":{"pk":{"S":"order-1"}}}}]}}'
        request._alternator_query_plan = None

        update_endpoint = handlers["update_endpoint"]
        update_endpoint(request)
        first_url = request.url
        update_endpoint(request)
        second_url = request.url
        update_endpoint(request)
        third_url = request.url
        update_endpoint(request)
        fourth_url = request.url

        assert (first_url, second_url, third_url, fourth_url) == (
            "http://c:8000/",
            "http://a:8000/",
            "http://b:8000/",
            "http://c:8000/",
        )


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

    def test_malformed_multi_type_value_is_not_routable(self) -> None:
        """AttributeValue unions with multiple members use normal routing."""
        params = {"Key": {"pk": {"S": "value", "N": "1"}}}

        assert extract_partition_key(params, "pk") is None

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

    def test_extract_from_invalid_batch_write_union_is_ignored(self) -> None:
        """Batch write entries with both operations are not routing targets."""
        params = {
            "RequestItems": {
                "orders": [
                    {
                        "PutRequest": {"Item": {"pk": {"S": "put"}}},
                        "DeleteRequest": {"Key": {"pk": {"S": "delete"}}},
                    },
                ],
            }
        }

        assert get_table_name(params) is None
        assert extract_partition_key(params, "pk") is None

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

    def test_blocking_lookup_discovers_on_first_call(self) -> None:
        """Diagnostic lookups preserve first-call synchronous discovery."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
        }
        cache = PartitionKeyCache(mock_client)

        try:
            assert cache.get_pk_name("test_table") == "pk"
            mock_client.describe_table.assert_called_once_with(TableName="test_table")
        finally:
            cache.close()

    def test_blocking_lookup_owner_uses_sdk_timeout_not_join_timeout(self) -> None:
        """The initiating diagnostic lookup waits through a slow SDK success."""
        mock_client = MagicMock()

        def describe_table(*, TableName: str) -> dict[str, Any]:
            assert TableName == "test_table"
            time.sleep(0.05)
            return {
                "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
            }

        mock_client.describe_table.side_effect = describe_table
        cache = PartitionKeyCache(mock_client)

        try:
            with patch(
                "alternator.core.key_affinity.PK_DISCOVERY_TIMEOUT_SECONDS",
                0.001,
            ):
                assert cache.get_pk_name("test_table") == "pk"
            mock_client.describe_table.assert_called_once_with(TableName="test_table")
        finally:
            cache.close()

    def test_blocking_concurrent_lookups_share_discovery(self) -> None:
        """Concurrent diagnostic lookups wait for one DescribeTable call."""
        mock_client = MagicMock()
        started = threading.Event()
        release = threading.Event()

        def describe_table(*, TableName: str) -> dict[str, Any]:
            assert TableName == "test_table"
            started.set()
            assert release.wait(timeout=1)
            return {
                "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
            }

        mock_client.describe_table.side_effect = describe_table
        cache = PartitionKeyCache(mock_client)
        try:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(cache.get_pk_name, "test_table") for _ in range(5)
                ]
                assert started.wait(timeout=1)
                release.set()
                assert [future.result(timeout=1) for future in futures] == ["pk"] * 5
            mock_client.describe_table.assert_called_once_with(TableName="test_table")
        finally:
            release.set()
            cache.close()

    def test_worker_start_failure_falls_back_and_can_retry(self) -> None:
        """Optional discovery does not fail requests when no thread can start."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {
            "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
        }
        cache = PartitionKeyCache(mock_client)

        try:
            with patch.object(
                threading.Thread,
                "start",
                side_effect=RuntimeError("can't start new thread"),
            ):
                assert cache.get_cached_pk_name("test_table") is None

            assert cache._pending == {}
            assert cache._worker._queue.empty()
            mock_client.describe_table.assert_not_called()

            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: cache.get_cached_pk_name("test_table") == "pk")
            mock_client.describe_table.assert_called_once_with(TableName="test_table")
        finally:
            cache.close()

    def test_cache_miss_discovers_in_background(self) -> None:
        """First and in-flight misses fall back without waiting for discovery."""
        mock_client = MagicMock()
        started = threading.Event()
        release = threading.Event()

        def describe_table(*, TableName: str) -> dict[str, Any]:
            assert TableName == "test_table"
            started.set()
            assert release.wait(timeout=1)
            return {
                "Table": {
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ]
                }
            }

        mock_client.describe_table.side_effect = describe_table

        cache = PartitionKeyCache(mock_client, shutdown_timeout_seconds=0.05)
        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert started.wait(timeout=1)
            assert cache.get_cached_pk_name("test_table") is None

            release.set()
            assert _wait_until(lambda: cache.get_cached_pk_name("test_table") == "pk")
            mock_client.describe_table.assert_called_once_with(TableName="test_table")
        finally:
            release.set()
            cache.close()

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
        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: cache.get_cached_pk_name("test_table") == "pk")
            assert cache.get_cached_pk_name("test_table") == "pk"
            assert mock_client.describe_table.call_count == 1
        finally:
            cache.close()

    def test_preload_populates_cache(self) -> None:
        """Test preload populates cache without API calls."""
        mock_client = MagicMock()
        cache = PartitionKeyCache(mock_client)
        try:
            cache.preload({"users": "user_id", "orders": "order_id"})
            assert cache.get_cached_pk_name("users") == "user_id"
            assert cache.get_cached_pk_name("orders") == "order_id"
            mock_client.describe_table.assert_not_called()
        finally:
            cache.close()

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
        try:
            cache.preload({"test_table": "pk"})
            cache.clear()
            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: cache.get_cached_pk_name("test_table") == "pk")
            mock_client.describe_table.assert_called_once()
        finally:
            cache.close()

    def test_handles_describe_table_error(self) -> None:
        """Test graceful handling of DescribeTable errors."""
        mock_client = MagicMock()
        mock_client.describe_table.side_effect = Exception("Access denied")

        cache = PartitionKeyCache(mock_client)
        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: mock_client.describe_table.call_count == 1)
        finally:
            cache.close()

    def test_handles_missing_key_schema(self) -> None:
        """Test handling when KeySchema is missing."""
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {"Table": {}}

        cache = PartitionKeyCache(mock_client)
        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: mock_client.describe_table.call_count == 1)
        finally:
            cache.close()

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
        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert _wait_until(lambda: mock_client.describe_table.call_count == 1)
        finally:
            cache.close()

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
        try:
            assert cache.get_cached_pk_name("composite_table") is None
            assert _wait_until(
                lambda: cache.get_cached_pk_name("composite_table") == "pk"
            )
        finally:
            cache.close()

    def test_queue_is_bounded_and_overflow_can_retry(self) -> None:
        """Distinct-table floods cannot create an unbounded work backlog."""
        mock_client = MagicMock()
        first_started = threading.Event()
        release_first = threading.Event()
        calls: list[str] = []

        def describe_table(*, TableName: str) -> dict[str, Any]:
            calls.append(TableName)
            if TableName == "active":
                first_started.set()
                assert release_first.wait(timeout=1)
            return {
                "Table": {
                    "KeySchema": [
                        {"AttributeName": f"pk_{TableName}", "KeyType": "HASH"}
                    ]
                }
            }

        mock_client.describe_table.side_effect = describe_table
        cache = PartitionKeyCache(mock_client, queue_capacity=1)
        try:
            assert cache.get_cached_pk_name("active") is None
            assert first_started.wait(timeout=1)
            assert cache.get_cached_pk_name("queued") is None
            assert cache.get_cached_pk_name("overflow") is None

            release_first.set()
            assert _wait_until(
                lambda: cache.get_cached_pk_name("active") == "pk_active"
            )
            assert _wait_until(
                lambda: cache.get_cached_pk_name("queued") == "pk_queued"
            )
            assert "overflow" not in calls

            assert cache.get_cached_pk_name("overflow") is None
            assert _wait_until(
                lambda: cache.get_cached_pk_name("overflow") == "pk_overflow"
            )
        finally:
            release_first.set()
            cache.close()

    def test_close_is_bounded_cancels_queue_and_is_idempotent(self) -> None:
        """Close rejects new work and does not wait forever on active I/O."""
        mock_client = MagicMock()
        first_started = threading.Event()
        release_first = threading.Event()
        close_returned = threading.Event()
        calls: list[str] = []

        def describe_table(*, TableName: str) -> dict[str, Any]:
            calls.append(TableName)
            first_started.set()
            assert release_first.wait(timeout=1)
            return {
                "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
            }

        mock_client.describe_table.side_effect = describe_table
        cache = PartitionKeyCache(mock_client, shutdown_timeout_seconds=0.01)
        assert cache.get_cached_pk_name("active") is None
        assert first_started.wait(timeout=1)
        assert cache.get_cached_pk_name("queued") is None

        def close_cache() -> None:
            cache.close()
            close_returned.set()

        closer = threading.Thread(target=close_cache)
        closer.start()
        assert close_returned.wait(timeout=0.5)
        closer.join()
        cache.close()
        assert cache.get_cached_pk_name("new") is None

        release_first.set()
        worker_thread = cache._worker._thread
        assert worker_thread is not None
        worker_thread.join(timeout=1)
        assert not worker_thread.is_alive()
        assert calls == ["active"]

    def test_close_wakes_blocking_lookup_waiters(self) -> None:
        """Closing a cache releases diagnostics waiting on background work."""
        mock_client = MagicMock()
        fetch_started = threading.Event()
        release_fetch = threading.Event()
        entered_wait = threading.Event()
        waiter_done = threading.Event()
        results: list[str | None] = []

        def describe_table(*, TableName: str) -> dict[str, Any]:
            assert TableName == "test_table"
            fetch_started.set()
            assert release_fetch.wait(timeout=1)
            return {
                "Table": {"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]}
            }

        mock_client.describe_table.side_effect = describe_table
        cache = PartitionKeyCache(mock_client, shutdown_timeout_seconds=0.01)
        assert cache.get_cached_pk_name("test_table") is None
        assert fetch_started.wait(timeout=1)
        pending_event = cache._pending["test_table"]
        pending_wait = pending_event.wait

        def observed_wait(timeout: float | None = None) -> bool:
            entered_wait.set()
            return pending_wait(timeout)

        def wait_for_pk() -> None:
            results.append(cache.get_pk_name("test_table"))
            waiter_done.set()

        waiter = threading.Thread(target=wait_for_pk)
        with (
            patch.object(pending_event, "wait", side_effect=observed_wait),
            patch("alternator.core.key_affinity.PK_DISCOVERY_TIMEOUT_SECONDS", 1.0),
        ):
            waiter.start()
            assert entered_wait.wait(timeout=1)
            assert not waiter_done.is_set()
            cache.close()
            assert waiter_done.wait(timeout=0.5)

        waiter.join(timeout=1)
        assert results == [None]
        release_fetch.set()
        worker_thread = cache._worker._thread
        assert worker_thread is not None
        worker_thread.join(timeout=1)
        assert not worker_thread.is_alive()

    def test_client_gc_stops_idle_worker_without_retaining_owner(self) -> None:
        """Forgotten clients and caches remain collectable after discovery."""

        class Client:
            cache: PartitionKeyCache | None = None

            def describe_table(self, *, TableName: str) -> dict[str, Any]:
                return {
                    "Table": {
                        "KeySchema": [
                            {"AttributeName": f"pk_{TableName}", "KeyType": "HASH"}
                        ]
                    }
                }

        def create_owner_cycle() -> tuple[
            weakref.ReferenceType[Client],
            weakref.ReferenceType[PartitionKeyCache],
            threading.Thread,
        ]:
            client = Client()
            cache = PartitionKeyCache(client)  # type: ignore[arg-type]
            client.cache = cache
            assert cache.get_cached_pk_name("table") is None
            assert _wait_until(lambda: cache.get_cached_pk_name("table") == "pk_table")
            worker_thread = cache._worker._thread
            assert worker_thread is not None
            return weakref.ref(client), weakref.ref(cache), worker_thread

        client_ref, cache_ref, worker_thread = create_owner_cycle()
        for _ in range(20):
            gc.collect()
            if client_ref() is None and cache_ref() is None:
                break
            time.sleep(0.01)

        assert client_ref() is None
        assert cache_ref() is None
        worker_thread.join(timeout=1)
        assert not worker_thread.is_alive()


class TestPartitionKeyCacheThreadSafety:
    """Stress tests for PartitionKeyCache thread safety."""

    def test_concurrent_get_pk_name_same_table(self) -> None:
        """Test concurrent access to same table."""
        mock_client = MagicMock()
        started = threading.Event()
        release = threading.Event()

        def describe_table(*, TableName: str) -> dict[str, Any]:
            assert TableName == "test_table"
            started.set()
            assert release.wait(timeout=1)
            return {
                "Table": {
                    "KeySchema": [
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ]
                }
            }

        mock_client.describe_table.side_effect = describe_table

        cache = PartitionKeyCache(mock_client)
        errors: list[Exception] = []
        results: list[str | None] = []
        lock = threading.Lock()

        def get_pk() -> None:
            try:
                for _ in range(50):
                    result = cache.get_cached_pk_name("test_table")
                    with lock:
                        results.append(result)
            except Exception as e:
                errors.append(e)

        try:
            assert cache.get_cached_pk_name("test_table") is None
            assert started.wait(timeout=1)
            threads = [threading.Thread(target=get_pk) for _ in range(10)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            assert not errors
            assert len(results) == 500
            assert all(result is None for result in results)
            assert mock_client.describe_table.call_count == 1
            release.set()
            assert _wait_until(lambda: cache.get_cached_pk_name("test_table") == "pk")
        finally:
            release.set()
            cache.close()

    def test_concurrent_get_pk_name_different_tables(self) -> None:
        """One worker serializes discovery across different tables."""
        mock_client = MagicMock()
        first_started = threading.Event()
        release = threading.Event()
        activity_lock = threading.Lock()
        active = 0
        max_active = 0

        def describe_table_side_effect(TableName: str) -> dict[str, object]:
            nonlocal active, max_active
            with activity_lock:
                active += 1
                max_active = max(max_active, active)
            first_started.set()
            assert release.wait(timeout=1)
            try:
                return {
                    "Table": {
                        "KeySchema": [
                            {
                                "AttributeName": f"pk_{TableName}",
                                "KeyType": "HASH",
                            },
                        ]
                    }
                }
            finally:
                with activity_lock:
                    active -= 1

        mock_client.describe_table.side_effect = describe_table_side_effect

        cache = PartitionKeyCache(mock_client)
        table_names = [f"table_{index}" for index in range(10)]

        def has_expected_pk(table_name: str) -> Callable[[], bool]:
            return lambda: cache.get_cached_pk_name(table_name) == f"pk_{table_name}"

        try:
            with ThreadPoolExecutor(max_workers=10) as executor:
                results = list(executor.map(cache.get_cached_pk_name, table_names))
            assert all(result is None for result in results)
            assert first_started.wait(timeout=1)
            release.set()
            for table_name in table_names:
                assert _wait_until(has_expected_pk(table_name))
            assert max_active == 1
            assert mock_client.describe_table.call_count == len(table_names)
        finally:
            release.set()
            cache.close()


class TestAffinitySelectorConcurrency:
    """Stress tests for AffinitySelector."""

    def test_high_concurrency_determinism(self) -> None:
        """Test that selection remains deterministic under high concurrency."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("a", "b", "c", "d", "e"), scope_name="test")

        # Pre-compute expected results
        expected = {h: selector.select(nodes, h) for h in range(20)}

        with ThreadPoolExecutor(max_workers=10) as executor:
            for h in range(20):
                futures = [
                    executor.submit(selector.select, nodes, h) for _ in range(10)
                ]
                results = [f.result() for f in futures]
                assert all(r == expected[h] for r in results)

    def test_concurrent_selection_distribution(self) -> None:
        """Test hash distribution under high concurrency."""
        selector = AffinitySelector()
        nodes = NodeList(nodes=("n1", "n2", "n3", "n4"), scope_name="test")

        # Use different hash values
        hashes = list(range(400))

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(selector.select, nodes, h) for h in hashes]
            results = [f.result() for f in futures]

        counter: Counter[str | None] = Counter(results)

        # Distribution should be roughly even
        for node in ("n1", "n2", "n3", "n4"):
            assert 60 < counter[node] < 140
