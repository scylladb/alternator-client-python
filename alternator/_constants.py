"""Shared constants for Alternator client modules."""

# Attribute names used to store manager and pk_cache references on boto3 clients
MANAGER_ATTR = "_alternator_manager"
MANAGER_OWNS_ATTR = "_alternator_manager_owns_lifecycle"
PK_CACHE_ATTR = "_alternator_pk_cache"

# Timeout in seconds for waiting on partition key discovery from concurrent requests
PK_DISCOVERY_TIMEOUT_SECONDS = 10.0
