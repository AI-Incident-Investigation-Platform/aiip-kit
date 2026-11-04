import asyncio
import random
import time
from collections.abc import Collection
from dataclasses import dataclass
from typing import Protocol

import httpx

from aiip_kit.errors import DependencyUnavailableError

TOKEN_PATH = "/internal/v1/oauth/token"


class ServiceTokenProvider(Protocol):
    async def token(self, audience: str, scopes: Collection[str]) -> str:
        """Служебный JWT для вызова сервиса `audience`.

        Raises:
            DependencyUnavailableError: `auth` недоступен или отказал.
        """
        ...


@dataclass
class _Cached:
    token: str
    refresh_at: float


class AuthServiceTokenProvider:
    """Client credentials у `auth`; токен в памяти, обновляется за ~60 с до истечения.

    Одновременные запросы одного токена идут в `auth` одним вызовом. Токен в
    outbox и логи не попадает: берётся прямо перед HTTP-вызовом.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        auth_url: str,
        client_id: str,
        client_secret: str,
        timeout_seconds: float = 2.0,
    ) -> None:
        self._client = client
        self._url = f"{auth_url.rstrip('/')}{TOKEN_PATH}"
        self._credentials = httpx.BasicAuth(client_id, client_secret)
        self._timeout = timeout_seconds
        self._cache: dict[tuple[str, str], _Cached] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}

    async def token(self, audience: str, scopes: Collection[str]) -> str:
        key = (audience, " ".join(sorted(scopes)))
        if (cached := self._cache.get(key)) is not None and time.monotonic() < cached.refresh_at:
            return cached.token
        async with self._locks.setdefault(key, asyncio.Lock()):
            cached = self._cache.get(key)
            if cached is None or time.monotonic() >= cached.refresh_at:
                cached = await self._issue(*key)
                self._cache[key] = cached
            return cached.token

    async def _issue(self, audience: str, scope: str) -> _Cached:
        try:
            response = await self._client.post(
                self._url,
                auth=self._credentials,
                data={"grant_type": "client_credentials", "audience": audience, "scope": scope},
                timeout=self._timeout,
            )
            response.raise_for_status()
            body = response.json()
            expires_in = float(body["expires_in"])
            token = str(body["access_token"])
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise DependencyUnavailableError("auth") from exc
        # обновление на ~80% срока с разбросом: поды не идут в auth одновременно
        lifetime = max(expires_in - 60, expires_in * 0.5) * random.uniform(0.9, 1.0)
        return _Cached(token=token, refresh_at=time.monotonic() + lifetime)
