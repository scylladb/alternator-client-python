"""TLS configuration utilities."""

from __future__ import annotations

import ssl
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alternator.config import TLS


def create_ssl_context(tls_config: TLS) -> ssl.SSLContext:
    """
    Create an SSL context from TLS configuration.

    Args:
        tls_config: TLS configuration settings

    Returns:
        Configured SSL context
    """
    if tls_config.trust_all_certificates:
        # INSECURE - for development only
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    # Create context with verification
    context = ssl.create_default_context()

    # Configure hostname verification
    context.check_hostname = tls_config.verify_hostname

    # Load custom CA certificates
    if tls_config.custom_ca_cert_paths:
        for cert_path in tls_config.custom_ca_cert_paths:
            context.load_verify_locations(str(cert_path))

    # If not using system CA and custom certs provided, don't load system certs
    if not tls_config.trust_system_ca_certs and tls_config.custom_ca_cert_paths:
        context.verify_mode = ssl.CERT_REQUIRED

    # Configure session caching
    # Note: Python's ssl module only exposes session ticket control (OP_NO_TICKET).
    # The cache_size and timeout_seconds settings in TlsSessionCacheConfig are
    # reserved for future use with custom implementations or alternative TLS backends.
    if tls_config.session_cache.enabled:
        # Enable session tickets for session reuse
        context.options &= ~ssl.OP_NO_TICKET
    else:
        # Disable session tickets
        context.options |= ssl.OP_NO_TICKET

    return context
