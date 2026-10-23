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

"""Explicit factories for the same contract suite across Python/native clients."""

from __future__ import annotations

import asyncio
import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from alternator import Auth, Config


@dataclass
class BackendClient:
    """Expose one awaitable invocation shape for either factory family."""

    raw: Any
    asynchronous: bool

    async def call(self, method: str, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401 -- SDK kwargs
        function = getattr(self.raw, method)
        if self.asynchronous:
            result: dict[str, Any] = await function(**kwargs)
            return result
        return await asyncio.to_thread(function, **kwargs)


@dataclass(frozen=True)
class BackendFactory:
    """Native factories are loaded only when explicitly selected by the runner.

    A selected missing native wheel is an error, never a skip or Python fallback.
    """

    name: str

    @property
    def preserves_full_error_payload(self) -> bool:
        """Native responses retain unmodeled body fields beyond Python parsing."""
        return self.name.startswith("rust-")

    @asynccontextmanager
    async def client(
        self, config: Config, *, auth: Auth | None = None
    ) -> AsyncIterator[BackendClient]:
        asynchronous = self.name.endswith("async")
        if self.name.startswith("python-"):
            module = importlib.import_module(
                "alternator.async_client" if asynchronous else "alternator"
            )
            create = (
                module.create_async_client if asynchronous else module.create_client
            )
            close = module.close_async_client if asynchronous else module.close_client
        elif self.name.startswith("rust-"):
            module = importlib.import_module(
                "alternator._experimental.rust.async_client"
                if asynchronous
                else "alternator._experimental.rust"
            )
            create = module.create_client
            close = module.close_client
        else:
            raise ValueError(f"unknown contract backend {self.name}")
        raw = (
            await create(config, auth=auth)
            if asynchronous
            else await asyncio.to_thread(create, config, auth=auth)
        )
        try:
            yield BackendClient(raw, asynchronous)
        finally:
            if asynchronous:
                await close(raw)
            else:
                await asyncio.to_thread(close, raw)
