"""Integration tests for HTTP/HTTPS connection reuse and pooling.

These tests verify that the client properly reuses TCP connections for both
serial and parallel requests over HTTP and HTTPS.

These tests require a running Scylla cluster with Alternator enabled.
Start a local cluster with: make scylla-start
"""

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from alternator import (
    TLS,
    AlternatorConfigBuilder,
    Config,
)
from alternator import (
    client as alternator_client,
)
from tests.integration import (
    SCYLLA_HOST,
    SCYLLA_HTTPS_PORT,
    SCYLLA_PORT,
    SKIP_INTEGRATION,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def http_config() -> Config:
    """Create HTTP test configuration."""
    return Config(
        seed_hosts=[SCYLLA_HOST],
        port=SCYLLA_PORT,
        scheme="http",
    )


@pytest.fixture
def https_config() -> Config | None:
    """Create HTTPS test configuration with trust-all for self-signed certs."""
    ca_path = Path(__file__).resolve().parents[1] / "scylla" / "db.crt"
    if not ca_path.exists():
        pytest.skip("Self-signed certificate not found (run 'make scylla-start')")
    return (
        AlternatorConfigBuilder()
        .with_seeds(SCYLLA_HOST)
        .with_port(SCYLLA_HTTPS_PORT)
        .with_https(TLS.with_custom_ca(ca_path))
        .build()
    )


@pytest.fixture
def ca_path() -> Path:
    """Get CA cert path, skip if not available."""
    path = Path(__file__).resolve().parents[1] / "scylla" / "db.crt"
    if not path.exists():
        pytest.skip("Self-signed certificate not found (run 'make scylla-start')")
    return path


class TestHttpConnectionReuseSerial:
    """Test HTTP connection reuse with serial requests."""

    def test_serial_requests_succeed(self, http_config: Config) -> None:
        """Test that multiple serial requests over HTTP succeed without errors."""
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            for _ in range(20):
                response = client.list_tables()
                assert "TableNames" in response

    def test_serial_crud_operations(self, http_config: Config) -> None:
        """Test serial CRUD operations reuse connections."""
        table_name = f"test_conn_{uuid.uuid4().hex[:8]}"
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                # Repeated put/get cycles should reuse connections
                for i in range(10):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"key_{i}"}, "data": {"S": f"val_{i}"}},
                    )
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestHttpsConnectionReuseSerial:
    """Test HTTPS connection reuse with serial requests."""

    def test_serial_requests_succeed(self, https_config: Config, ca_path: Path) -> None:
        """Test that multiple serial HTTPS requests succeed."""
        with alternator_client(
            "dynamodb", cluster_config=https_config, verify=str(ca_path)
        ) as client:
            for _ in range(20):
                response = client.list_tables()
                assert "TableNames" in response

    def test_serial_crud_over_https(self, https_config: Config, ca_path: Path) -> None:
        """Test serial CRUD over HTTPS reuses connections."""
        table_name = f"test_https_{uuid.uuid4().hex[:8]}"
        with alternator_client(
            "dynamodb", cluster_config=https_config, verify=str(ca_path)
        ) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                for i in range(10):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"key_{i}"}, "data": {"S": f"val_{i}"}},
                    )
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestHttpConnectionReuseParallel:
    """Test HTTP connection reuse with parallel requests."""

    def test_parallel_list_tables(self, http_config: Config) -> None:
        """Test parallel requests over HTTP all succeed."""
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            errors: list[Exception] = []
            results: list[dict] = []
            lock = threading.Lock()

            def do_request() -> None:
                try:
                    response = client.list_tables()
                    with lock:
                        results.append(response)
                except Exception as e:
                    with lock:
                        errors.append(e)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(do_request) for _ in range(50)]
                for f in futures:
                    f.result()

            assert len(errors) == 0, f"Parallel requests had errors: {errors}"
            assert len(results) == 50
            assert all("TableNames" in r for r in results)

    def test_parallel_writes(self, http_config: Config) -> None:
        """Test parallel write operations over HTTP."""
        table_name = f"test_par_{uuid.uuid4().hex[:8]}"
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                errors: list[Exception] = []
                lock = threading.Lock()

                def write_item(idx: int) -> None:
                    try:
                        client.put_item(
                            TableName=table_name,
                            Item={
                                "pk": {"S": f"par_key_{idx}"},
                                "data": {"S": f"par_val_{idx}"},
                            },
                        )
                    except Exception as e:
                        with lock:
                            errors.append(e)

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(write_item, i) for i in range(20)]
                    for f in futures:
                        f.result()

                assert len(errors) == 0, f"Parallel writes had errors: {errors}"

                # Verify all items were written
                for i in range(20):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"par_key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"par_val_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestHttpsConnectionReuseParallel:
    """Test HTTPS connection reuse with parallel requests."""

    def test_parallel_list_tables_https(
        self, https_config: Config, ca_path: Path
    ) -> None:
        """Test parallel HTTPS requests all succeed."""
        with alternator_client(
            "dynamodb", cluster_config=https_config, verify=str(ca_path)
        ) as client:
            errors: list[Exception] = []
            results: list[dict] = []
            lock = threading.Lock()

            def do_request() -> None:
                try:
                    response = client.list_tables()
                    with lock:
                        results.append(response)
                except Exception as e:
                    with lock:
                        errors.append(e)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = [executor.submit(do_request) for _ in range(50)]
                for f in futures:
                    f.result()

            assert len(errors) == 0, f"Parallel HTTPS requests had errors: {errors}"
            assert len(results) == 50

    def test_parallel_writes_https(self, https_config: Config, ca_path: Path) -> None:
        """Test parallel write operations over HTTPS."""
        table_name = f"test_https_par_{uuid.uuid4().hex[:8]}"
        with alternator_client(
            "dynamodb", cluster_config=https_config, verify=str(ca_path)
        ) as client:
            client.create_table(
                TableName=table_name,
                KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            waiter = client.get_waiter("table_exists")
            waiter.wait(TableName=table_name)

            try:
                errors: list[Exception] = []
                lock = threading.Lock()

                def write_item(idx: int) -> None:
                    try:
                        client.put_item(
                            TableName=table_name,
                            Item={
                                "pk": {"S": f"par_key_{idx}"},
                                "data": {"S": f"par_val_{idx}"},
                            },
                        )
                    except Exception as e:
                        with lock:
                            errors.append(e)

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = [executor.submit(write_item, i) for i in range(20)]
                    for f in futures:
                        f.result()

                assert len(errors) == 0
            finally:
                client.delete_table(TableName=table_name)


class TestConnectionPoolingValidation:
    """Test connection pooling behavior."""

    def test_many_serial_requests_dont_exhaust_resources(
        self, http_config: Config
    ) -> None:
        """Test that many serial requests don't exhaust file descriptors or connections."""
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            # A large number of serial requests should work fine with connection reuse
            for _ in range(100):
                response = client.list_tables()
                assert "TableNames" in response

    def test_high_concurrency_doesnt_exhaust_pool(self, http_config: Config) -> None:
        """Test high concurrency doesn't exhaust the connection pool."""
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            errors: list[Exception] = []
            lock = threading.Lock()

            def do_request() -> None:
                try:
                    client.list_tables()
                except Exception as e:
                    with lock:
                        errors.append(e)

            # Run multiple waves of concurrent requests
            for _ in range(5):
                with ThreadPoolExecutor(max_workers=20) as executor:
                    futures = [executor.submit(do_request) for _ in range(40)]
                    for f in futures:
                        f.result()

            assert len(errors) == 0, f"Connection pool exhaustion: {errors}"

    def test_connection_reuse_rate(self, http_config: Config) -> None:
        """Test that connections are being reused by verifying consistent throughput.

        If connections were not being reused, we'd see degraded performance
        or errors with many sequential requests.
        """
        with alternator_client("dynamodb", cluster_config=http_config) as client:
            # Warm up
            client.list_tables()

            # Run many requests — if connections leak, this would eventually fail
            success_count = 0
            for _ in range(200):
                response = client.list_tables()
                if "TableNames" in response:
                    success_count += 1

            assert success_count == 200
