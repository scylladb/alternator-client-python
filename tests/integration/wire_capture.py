"""Helpers for inspecting botocore requests immediately before transport send."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CapturedRequest:
    """Prepared request snapshot captured immediately before transport send."""

    headers: dict[str, str]
    body: bytes

    def header(self, name: str) -> str | None:
        """Return a case-insensitive header value."""
        return self.headers.get(name.lower())


def body_as_bytes(body: object) -> bytes:
    if body is None:
        return b""
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    if isinstance(body, bytearray | memoryview):
        return bytes(body)
    return b""


def normalize_headers(headers: object) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for raw_key, raw_value in dict(headers).items():
        key = raw_key.decode("utf-8") if isinstance(raw_key, bytes) else str(raw_key)
        value = (
            raw_value.decode("utf-8")
            if isinstance(raw_value, bytes)
            else str(raw_value)
        )
        normalized[key.lower()] = value
    return normalized


def capture_prepared_requests(client: Any) -> list[CapturedRequest]:  # noqa: ANN401 -- SDK clients are dynamically typed
    captured: list[CapturedRequest] = []

    def capture(request: Any, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
        captured.append(
            CapturedRequest(
                headers=normalize_headers(request.headers),
                body=body_as_bytes(getattr(request, "body", None)),
            )
        )

    client.meta.events.register_last("before-send.dynamodb.*", capture)
    return captured


def inject_custom_headers(client: Any) -> None:  # noqa: ANN401 -- SDK clients are dynamically typed
    def add_headers(request: Any, **kwargs: Any) -> None:  # noqa: ANN401 -- botocore event handler signature
        request.headers["X-Keep-Me"] = "keep"
        request.headers["X-Drop-Me"] = "drop"

    client.meta.events.register_first("before-send.dynamodb.*", add_headers)


def latest_request(captured: list[CapturedRequest]) -> CapturedRequest:
    assert captured, "expected at least one prepared request to be captured"
    return captured[-1]
