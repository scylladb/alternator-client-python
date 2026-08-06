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

"""Tests for request parsing utilities."""

import json
from unittest.mock import MagicMock

from alternator.core.request import extract_operation_name, extract_request_params


class TestExtractOperationInfo:
    """Tests for extract_operation_name and extract_request_params functions."""

    def test_extract_operation_name_from_target_header(self) -> None:
        """Test extracting operation name from X-Amz-Target header."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.PutItem"}
        request.body = b"{}"

        operation_name = extract_operation_name(request)

        assert operation_name == "PutItem"

    def test_extract_operation_name_bytes_header(self) -> None:
        """Test extracting operation name when header is bytes."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": b"DynamoDB_20120810.GetItem"}
        request.body = b"{}"

        operation_name = extract_operation_name(request)

        assert operation_name == "GetItem"

    def test_extract_params_from_body(self) -> None:
        """Test extracting parameters from JSON body."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.PutItem"}
        request.body = json.dumps(
            {
                "TableName": "my_table",
                "Item": {"pk": {"S": "123"}, "data": {"S": "hello"}},
            }
        ).encode("utf-8")

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "PutItem"
        assert params["TableName"] == "my_table"
        assert params["Item"]["pk"]["S"] == "123"

    def test_extract_params_string_body(self) -> None:
        """Test extracting parameters from string body."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.GetItem"}
        request.body = '{"TableName": "test_table", "Key": {"id": {"S": "abc"}}}'

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "GetItem"
        assert params["TableName"] == "test_table"
        assert params["Key"]["id"]["S"] == "abc"

    def test_missing_target_header(self) -> None:
        """Test handling missing X-Amz-Target header."""
        request = MagicMock()
        request.headers = {}
        request.body = b"{}"

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == ""
        assert params == {}

    def test_invalid_target_header_format(self) -> None:
        """Test handling invalid X-Amz-Target header format."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "InvalidFormat"}
        request.body = b"{}"

        operation_name = extract_operation_name(request)

        assert operation_name == ""

    def test_empty_body(self) -> None:
        """Test handling empty body."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.ListTables"}
        request.body = b""

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "ListTables"
        assert params == {}

    def test_none_body(self) -> None:
        """Test handling None body."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.ListTables"}
        request.body = None

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "ListTables"
        assert params == {}

    def test_invalid_json_body(self) -> None:
        """Test handling invalid JSON body."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.PutItem"}
        request.body = b"not valid json"

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "PutItem"
        assert params == {}

    def test_unicode_body(self) -> None:
        """Test handling body with unicode characters."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.PutItem"}
        request.body = json.dumps(
            {
                "TableName": "my_table",
                "Item": {"name": {"S": "こんにちは"}},
            }
        ).encode("utf-8")

        params = extract_request_params(request)

        assert params["Item"]["name"]["S"] == "こんにちは"

    def test_update_item_operation(self) -> None:
        """Test extracting UpdateItem operation info."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.UpdateItem"}
        request.body = json.dumps(
            {
                "TableName": "my_table",
                "Key": {"pk": {"S": "123"}},
                "UpdateExpression": "SET #d = :val",
                "ConditionExpression": "attribute_exists(pk)",
                "ExpressionAttributeNames": {"#d": "data"},
                "ExpressionAttributeValues": {":val": {"S": "updated"}},
            }
        ).encode("utf-8")

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "UpdateItem"
        assert params["TableName"] == "my_table"
        assert params["ConditionExpression"] == "attribute_exists(pk)"

    def test_delete_item_operation(self) -> None:
        """Test extracting DeleteItem operation info."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.DeleteItem"}
        request.body = json.dumps(
            {
                "TableName": "my_table",
                "Key": {"pk": {"S": "123"}},
                "ReturnValues": "ALL_OLD",
            }
        ).encode("utf-8")

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "DeleteItem"
        assert params["ReturnValues"] == "ALL_OLD"

    def test_query_operation(self) -> None:
        """Test extracting Query operation info."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.Query"}
        request.body = json.dumps(
            {
                "TableName": "my_table",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "123"}},
            }
        ).encode("utf-8")

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "Query"
        assert params["KeyConditionExpression"] == "pk = :pk"

    def test_batch_write_item_operation(self) -> None:
        """Test extracting BatchWriteItem operation info."""
        request = MagicMock()
        request.headers = {"X-Amz-Target": "DynamoDB_20120810.BatchWriteItem"}
        request.body = json.dumps(
            {
                "RequestItems": {
                    "my_table": [
                        {"PutRequest": {"Item": {"pk": {"S": "1"}}}},
                        {"DeleteRequest": {"Key": {"pk": {"S": "2"}}}},
                    ]
                }
            }
        ).encode("utf-8")

        operation_name = extract_operation_name(request)
        params = extract_request_params(request)

        assert operation_name == "BatchWriteItem"
        assert "RequestItems" in params
        assert "my_table" in params["RequestItems"]
