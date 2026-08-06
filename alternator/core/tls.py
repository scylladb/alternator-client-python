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
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        # INSECURE - for development only
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    else:
        if tls_config.trust_system_ca_certs:
            context = ssl.create_default_context()
        else:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.verify_mode = ssl.CERT_REQUIRED

        # Configure hostname verification
        context.check_hostname = tls_config.verify_hostname

    if not tls_config.trust_all_certificates and tls_config.custom_ca_cert_paths:
        for cert_path in tls_config.custom_ca_cert_paths:
            context.load_verify_locations(str(cert_path))

    if tls_config.client_cert_path is not None:
        context.load_cert_chain(
            certfile=str(tls_config.client_cert_path),
            keyfile=(
                str(tls_config.client_key_path)
                if tls_config.client_key_path is not None
                else None
            ),
        )

    if tls_config.key_log_file_path is not None and hasattr(context, "keylog_filename"):
        context.keylog_filename = str(tls_config.key_log_file_path)

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
