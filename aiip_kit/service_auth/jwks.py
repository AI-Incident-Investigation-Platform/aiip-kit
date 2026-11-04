import asyncio
import json
import time
from typing import Protocol

import httpx
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from jwt.algorithms import RSAAlgorithm

from aiip_kit.errors import DependencyUnavailableError


class KeyResolver(Protocol):
    async def get(self, kid: str) -> RSAPublicKey | None:
        """Ключ подписи по `kid`; неизвестный — `None`.

        Raises:
            DependencyUnavailableError: ключ неизвестен, а JWKS `auth` не получить.
        """
        ...


class JwksKeys:
    """Ключи из JWKS `auth` в памяти процесса.

    Неизвестный `kid` перечитывает JWKS не чаще `refresh_interval_seconds`:
    поток токенов с выдуманным `kid` не превращается в поток запросов к `auth`.
    """

    def __init__(
        self, client: httpx.AsyncClient, jwks_url: str, refresh_interval_seconds: float = 30
    ) -> None:
        self._client = client
        self._url = jwks_url
        self._interval = refresh_interval_seconds
        self._keys: dict[str, RSAPublicKey] = {}
        self._refreshed_at: float | None = None
        self._refreshing = asyncio.Lock()

    async def get(self, kid: str) -> RSAPublicKey | None:
        if (key := self._keys.get(kid)) is not None:
            return key
        async with self._refreshing:
            if kid not in self._keys and self._refresh_allowed():
                self._refreshed_at = time.monotonic()
                self._keys = await self._fetch()
        return self._keys.get(kid)

    def _refresh_allowed(self) -> bool:
        return self._refreshed_at is None or time.monotonic() - self._refreshed_at >= self._interval

    async def _fetch(self) -> dict[str, RSAPublicKey]:
        try:
            response = await self._client.get(self._url, timeout=2.0)
            response.raise_for_status()
            keys: dict[str, RSAPublicKey] = {}
            for jwk in response.json()["keys"]:
                key = RSAAlgorithm.from_jwk(json.dumps(jwk))
                if isinstance(key, RSAPublicKey):
                    keys[jwk["kid"]] = key
            return keys
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise DependencyUnavailableError("auth") from exc


class StaticKeys:
    """Ключи, заданные заранее: для тестов."""

    def __init__(self, keys: dict[str, RSAPublicKey]) -> None:
        self._keys = keys

    async def get(self, kid: str) -> RSAPublicKey | None:
        return self._keys.get(kid)
