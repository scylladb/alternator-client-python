#!/usr/bin/env python3
"""
Asynchronous usage demo for Alternator load balancing client.

This example shows how to:
- Create an async load-balanced client
- Perform concurrent DynamoDB operations
- Use the async context manager for automatic cleanup
"""

import asyncio
import logging
import os
import sys
from typing import Any, cast

from botocore.exceptions import ClientError

from alternator import Config, NoNodesAvailableError
from alternator.async_client import AsyncAlternatorClient

# Enable logging to see load balancing in action
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


async def main() -> None:
    """Run the async demo."""
    # Configure the client
    config = Config(
        seed_hosts=[os.environ.get("SCYLLA_HOST", "localhost")],
        port=int(os.environ.get("SCYLLA_PORT", "8000")),
        scheme="http",  # Use "https" for TLS
    )

    try:
        # Use the async context manager for automatic cleanup
        async with AsyncAlternatorClient(config) as client:
            # List existing tables
            print("Listing tables...")
            list_response = await client.list_tables()
            tables = list_response.get("TableNames", [])
            print(f"Found {len(tables)} tables: {tables}")

            # Create a test table if it doesn't exist
            table_name = "alternator_async_demo"
            if table_name not in tables:
                print(f"Creating table {table_name}...")
                await client.create_table(
                    TableName=table_name,
                    KeySchema=[
                        {"AttributeName": "pk", "KeyType": "HASH"},
                    ],
                    AttributeDefinitions=[
                        {"AttributeName": "pk", "AttributeType": "S"},
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
                print("Waiting for table to be active...")
                for _ in range(30):
                    resp = await client.describe_table(TableName=table_name)
                    if resp["Table"]["TableStatus"] == "ACTIVE":
                        break
                    await asyncio.sleep(1)
                else:
                    print("Timed out waiting for table to become active")
                    return
                print("Table created successfully!")

            # Put items concurrently
            print("Writing items concurrently...")
            write_tasks = [
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": f"async_user_{i}"},
                        "data": {"S": f"Hello from concurrent item {i}"},
                    },
                )
                for i in range(10)
            ]
            await asyncio.gather(*write_tasks)
            print("Items written successfully!")

            # Read items concurrently
            print("Reading items concurrently...")
            read_tasks = [
                client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": f"async_user_{i}"}},
                )
                for i in range(10)
            ]
            responses = await asyncio.gather(*read_tasks)

            for i, response in enumerate(responses):
                item = cast(dict[str, Any], response.get("Item", {}))
                print(f"  async_user_{i}: {item.get('data', {}).get('S', 'N/A')}")

            print("Async demo complete!")

    except NoNodesAvailableError as e:
        print(f"No Alternator nodes available: {e}", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        print(f"DynamoDB error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
