#!/usr/bin/env python3
"""
Basic synchronous usage demo for Alternator load balancing client.

This example shows how to:
- Create a load-balanced client
- Perform basic DynamoDB operations
- Use the context manager for automatic cleanup
"""

import logging
import os
import sys

from botocore.exceptions import ClientError

from alternator import AlternatorClient, Config, NoNodesAvailableError

# Enable logging to see load balancing in action
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def main() -> None:
    """Run the sync demo."""
    # Configure the client
    config = Config(
        seed_hosts=[os.environ.get("SCYLLA_HOST", "localhost")],
        port=int(os.environ.get("SCYLLA_PORT", "8000")),
        scheme="http",  # Use "https" for TLS
    )

    try:
        # Use the context manager for automatic cleanup
        with AlternatorClient(config) as client:
            # List existing tables
            print("Listing tables...")
            response = client.list_tables()
            tables = response.get("TableNames", [])
            print(f"Found {len(tables)} tables: {tables}")

            # Create a test table if it doesn't exist
            table_name = "alternator_demo"
            if table_name not in tables:
                print(f"Creating table {table_name}...")
                client.create_table(
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
                waiter = client.get_waiter("table_exists")
                waiter.wait(TableName=table_name)
                print("Table created successfully!")

            # Put some items
            print("Writing items...")
            for i in range(5):
                client.put_item(
                    TableName=table_name,
                    Item={
                        "pk": {"S": f"user_{i}"},
                        "data": {"S": f"Hello from item {i}"},
                    },
                )
            print("Items written successfully!")

            # Read items back
            print("Reading items...")
            for i in range(5):
                response = client.get_item(
                    TableName=table_name,
                    Key={"pk": {"S": f"user_{i}"}},
                )
                item = response.get("Item", {})
                print(f"  user_{i}: {item.get('data', {}).get('S', 'N/A')}")

            print("Demo complete!")

    except NoNodesAvailableError as e:
        print(f"No Alternator nodes available: {e}", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        print(f"DynamoDB error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
