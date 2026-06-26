#!/usr/bin/env python3
"""
Compare an Alternator client with a regular AWS SDK DynamoDB client.

By default this script only calls Alternator. Use --include-aws to also call
AWS DynamoDB, which requires normal AWS SDK credentials and region setup.
"""

from __future__ import annotations

import argparse
from typing import Protocol

import boto3

import alternator
from alternator import Auth


class DynamoDBLikeClient(Protocol):
    """Minimal DynamoDB client protocol used by this example."""

    @property
    def meta(self) -> object: ...

    def list_tables(self) -> dict[str, object]: ...


def print_client_details(name: str, client: DynamoDBLikeClient) -> None:
    """Print comparable client metadata."""
    meta = client.meta
    print(f"{name}:")
    print(f"  service: {meta.service_model.service_name}")
    print(f"  endpoint: {meta.endpoint_url}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare an Alternator client and a regular AWS SDK client."
    )
    parser.add_argument(
        "--seed",
        action="append",
        default=["localhost"],
        help="Alternator seed host without a port. Repeat for multiple seeds.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Single Alternator port used for every seed.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region for the regular DynamoDB client.",
    )
    parser.add_argument(
        "--include-aws",
        action="store_true",
        help="Also call AWS DynamoDB using normal AWS SDK credentials.",
    )
    parser.add_argument(
        "--alternator-key-id",
        help="Static Alternator access key id. If omitted, auth is disabled.",
    )
    parser.add_argument(
        "--alternator-secret",
        help="Static Alternator secret key. Required with --alternator-key-id.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    auth = Auth.disabled()
    if args.alternator_key_id:
        if not args.alternator_secret:
            raise SystemExit("--alternator-secret is required with --alternator-key-id")
        auth = Auth.static_credentials(args.alternator_key_id, args.alternator_secret)

    with alternator.client(
        seeds=args.seed,
        port=args.port,
        auth=auth,
        region_name=args.region,
    ) as alternator_client:
        print_client_details("Alternator", alternator_client)
        print(f"  tables: {alternator_client.list_tables()['TableNames']}")

    if args.include_aws:
        aws_client = boto3.client("dynamodb", region_name=args.region)
        print_client_details("AWS DynamoDB", aws_client)
        print(f"  tables: {aws_client.list_tables()['TableNames']}")
    else:
        print("AWS DynamoDB:")
        print(f"  client: boto3.client('dynamodb', region_name={args.region!r})")
        print("  tables: skipped; pass --include-aws to call AWS DynamoDB")


if __name__ == "__main__":
    main()
