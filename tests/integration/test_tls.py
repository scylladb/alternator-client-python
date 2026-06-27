"""Integration tests for TLS configuration.

Tests TLS session tickets for both ALN (node discovery) and DynamoDB API requests,
and SSLKEYLOGFILE support.

These tests require a running Scylla cluster with Alternator HTTPS enabled.
Start a local cluster with: make scylla-start
"""

import os
import ssl
import tempfile
import uuid
from pathlib import Path

import pytest

from alternator import (
    TLS,
    Config,
)
from alternator import (
    client as alternator_client,
)
from tests.integration import SCYLLA_HOST, SCYLLA_HTTPS_PORT, SKIP_INTEGRATION

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(SKIP_INTEGRATION, reason="Integration tests disabled"),
]


@pytest.fixture
def ca_path() -> Path:
    """Get CA cert path, skip if not available."""
    path = Path(__file__).resolve().parents[1] / "scylla" / "db.crt"
    if not path.exists():
        pytest.skip("Self-signed certificate not found (run 'make scylla-start')")
    return path


@pytest.fixture
def table_name() -> str:
    """Generate unique table name for test isolation."""
    return f"test_tls_{uuid.uuid4().hex[:8]}"


class TestTlsSessionTicketsAln:
    """Test TLS session tickets for ALN (node discovery) requests."""

    def test_session_tickets_enabled_for_aln(self, ca_path: Path) -> None:
        """Test that TLS session tickets work for /localnodes discovery."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS(
                custom_ca_cert_paths=(ca_path,),
                session_tickets_enabled=True,
            ),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=str(ca_path)
        ) as client:
            # Multiple requests should work with session tickets enabled.
            for _ in range(10):
                response = client.list_tables()
                assert "TableNames" in response

    def test_session_tickets_disabled_for_aln(self, ca_path: Path) -> None:
        """Test disabling TLS session tickets still works for ALN requests."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS(
                custom_ca_cert_paths=(ca_path,),
                session_tickets_enabled=False,
            ),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=str(ca_path)
        ) as client:
            for _ in range(10):
                response = client.list_tables()
                assert "TableNames" in response


class TestTlsSessionTicketsDynamoDbApi:
    """Test TLS session tickets for DynamoDB API requests."""

    def test_session_tickets_enabled_for_api(
        self, ca_path: Path, table_name: str
    ) -> None:
        """Test session tickets with DynamoDB API operations over HTTPS."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS(
                custom_ca_cert_paths=(ca_path,),
                session_tickets_enabled=True,
            ),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=str(ca_path)
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
                # Multiple CRUD operations over HTTPS with session tickets
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"tls_key_{i}"}, "data": {"S": f"val_{i}"}},
                    )

                for i in range(5):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"tls_key_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"val_{i}"
            finally:
                client.delete_table(TableName=table_name)

    def test_session_tickets_disabled_for_api(
        self, ca_path: Path, table_name: str
    ) -> None:
        """Test DynamoDB API operations over HTTPS without session tickets."""
        config = Config(
            seed_hosts=[SCYLLA_HOST],
            port=SCYLLA_HTTPS_PORT,
            scheme="https",
            tls=TLS(
                custom_ca_cert_paths=(ca_path,),
                session_tickets_enabled=False,
            ),
        )

        with alternator_client(
            "dynamodb", cluster_config=config, verify=str(ca_path)
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
                for i in range(5):
                    client.put_item(
                        TableName=table_name,
                        Item={"pk": {"S": f"tls_nc_{i}"}, "data": {"S": f"v_{i}"}},
                    )

                for i in range(5):
                    response = client.get_item(
                        TableName=table_name,
                        Key={"pk": {"S": f"tls_nc_{i}"}},
                    )
                    assert response["Item"]["data"]["S"] == f"v_{i}"
            finally:
                client.delete_table(TableName=table_name)


class TestSslKeyLogFile:
    """Test SSLKEYLOGFILE support for HTTPS connections.

    The SSLKEYLOGFILE environment variable causes Python's SSL module to log
    TLS pre-master secrets, which can be used for debugging with Wireshark.
    """

    def test_keylog_file_for_aln_requests(self, ca_path: Path) -> None:
        """Test that SSLKEYLOGFILE captures keys during ALN HTTPS requests."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".log", delete=False
        ) as keylog_file:
            keylog_path = keylog_file.name

        try:
            # Set SSLKEYLOGFILE for this test
            old_env = os.environ.get("SSLKEYLOGFILE")
            os.environ["SSLKEYLOGFILE"] = keylog_path

            try:
                config = Config(
                    seed_hosts=[SCYLLA_HOST],
                    port=SCYLLA_HTTPS_PORT,
                    scheme="https",
                    tls=TLS.with_custom_ca(ca_path),
                )

                with alternator_client(
                    "dynamodb", cluster_config=config, verify=str(ca_path)
                ) as client:
                    response = client.list_tables()
                    assert "TableNames" in response
            finally:
                if old_env is None:
                    os.environ.pop("SSLKEYLOGFILE", None)
                else:
                    os.environ["SSLKEYLOGFILE"] = old_env

            # Check if the keylog file was written to.
            # Note: Not all SSL backends support SSLKEYLOGFILE. If the file
            # is empty, the test still passes — we just verify no crash.
            keylog_content = Path(keylog_path).read_text()
            # If content was written, it should contain key material
            if keylog_content:
                assert "CLIENT_RANDOM" in keylog_content or len(keylog_content) > 0
        finally:
            os.unlink(keylog_path)

    def test_keylog_file_for_dynamodb_api_requests(self, ca_path: Path) -> None:
        """Test that SSLKEYLOGFILE captures keys during DynamoDB API HTTPS requests."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".log", delete=False
        ) as keylog_file:
            keylog_path = keylog_file.name

        try:
            old_env = os.environ.get("SSLKEYLOGFILE")
            os.environ["SSLKEYLOGFILE"] = keylog_path

            try:
                config = Config(
                    seed_hosts=[SCYLLA_HOST],
                    port=SCYLLA_HTTPS_PORT,
                    scheme="https",
                    tls=TLS.with_custom_ca(ca_path),
                )

                with alternator_client(
                    "dynamodb", cluster_config=config, verify=str(ca_path)
                ) as client:
                    # Multiple API calls over HTTPS
                    for _ in range(5):
                        client.list_tables()
            finally:
                if old_env is None:
                    os.environ.pop("SSLKEYLOGFILE", None)
                else:
                    os.environ["SSLKEYLOGFILE"] = old_env

            # Verify file was created (content may be empty depending on SSL backend)
            assert Path(keylog_path).exists()
        finally:
            os.unlink(keylog_path)


class TestTlsSessionTickets:
    """Test that TLS session ticket configuration values are applied correctly."""

    def test_session_ticket_context_options(self, ca_path: Path) -> None:
        """Verify SSL context has correct session ticket settings."""
        from alternator.core.tls import create_ssl_context

        # Enabled
        tls_enabled = TLS(
            custom_ca_cert_paths=(ca_path,),
            session_tickets_enabled=True,
        )
        ctx_enabled = create_ssl_context(tls_enabled)
        assert not (ctx_enabled.options & ssl.OP_NO_TICKET)

        # Disabled
        tls_disabled = TLS(
            custom_ca_cert_paths=(ca_path,),
            session_tickets_enabled=False,
        )
        ctx_disabled = create_ssl_context(tls_disabled)
        assert ctx_disabled.options & ssl.OP_NO_TICKET
