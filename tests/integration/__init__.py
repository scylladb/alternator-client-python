"""Integration tests for alternator package."""

import os

SCYLLA_HOST = os.environ.get("SCYLLA_HOST", "localhost")
SCYLLA_PORT = int(os.environ.get("SCYLLA_PORT", "9998"))
SCYLLA_HTTPS_PORT = int(os.environ.get("SCYLLA_HTTPS_PORT", "9999"))
SKIP_INTEGRATION = os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() in (
    "1",
    "true",
    "yes",
)
