import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol, Self
from uuid import UUID

import httpx
from redis.asyncio import Redis
from redis.exceptions import RedisError

from aiip_kit.errors import DependencyUnavailableError

TOKEN_PREFIX = "projects:token:"
TOKENS_SCOPE = "projects.tokens.read"


@dataclass(frozen=True)
class ApiTokenInfo:
    """API-токен проекта (`X-Ingest-Key`), `backend/decisions.md`, пункт 2."""

    state: Literal["active", "revoked", "expired"]
    token_id: UUID
    project_id: UUID
    environment_id: UUID | None
    scopes: frozenset[str]
    lifecycle_version: int
    ver: int
    expires_at: datetime | None
    observed_at_ms: int
    fresh_until_ms: int

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Self:
        return cls(
            state=value["state"],
            token_id=UUID(value["token_id"]),
            project_id=UUID(value["project_id"]),
            environment_id=UUID(value["environment_id"]) if value.get("environment_id") else None,
            scopes=frozenset(value["scopes"]),
            lifecycle_version=int(value["lifecycle_version"]),
            ver=int(value["ver"]),
            expires_at=datetime.fromisoformat(value["expires_at"])
            if value.get("expires_at")
            else None,
            observed_at_ms=int(value["observed_at_ms"]),
            fresh_until_ms=int(value["fresh_until_ms"]),
        )

    def allows(self, scope: str, now: datetime) -> bool:
        if self.state != "active" or scope not in self.scopes:
            return False
        return self.expires_at is None or now < self.expires_at


def token_digest(secret: str) -> str:
    return hashlib.sha256(secret.encode()).hexdigest()


class ApiTokenReader(Protocol):
    async def resolve(self, secret: str) -> ApiTokenInfo | None:
        """Токен по секрету из заголовка; неизвестный — `None`.

        Raises:
            DependencyUnavailableError: данные устарели, а `projects` недоступен.
        """
        ...


class DragonflyApiTokenReader:
    """`projects:token:<sha256>` в Dragonfly; нет или устарел — `projects` по HTTP.

    Ключ пишет `projects`; неизвестный digest он кэширует на секунду, поэтому
    перебор выдуманных токенов не превращается в поток запросов к нему.
    """

    def __init__(
        self,
        redis: Redis,
        client: httpx.AsyncClient,
        projects_url: str,
        token: Callable[[], Awaitable[str]],
        *,
        prefix: str = TOKEN_PREFIX,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._redis = redis
        self._client = client
        self._url = f"{projects_url.rstrip('/')}/internal/v1/api-tokens/"
        self._token = token
        self._prefix = prefix
        self._clock_ms = clock_ms

    async def resolve(self, secret: str) -> ApiTokenInfo | None:
        digest = token_digest(secret)
        try:
            raw = await self._redis.get(f"{self._prefix}{digest}")
        except RedisError as exc:
            raise DependencyUnavailableError("dragonfly") from exc
        if raw is not None:
            value = json.loads(raw)
            if int(value["fresh_until_ms"]) >= self._clock_ms():
                return ApiTokenInfo.from_json(value) if value.get("state") != "unknown" else None
        return await self._fetch(digest)

    async def _fetch(self, digest: str) -> ApiTokenInfo | None:
        try:
            response = await self._client.get(
                f"{self._url}{digest}",
                headers={"Authorization": f"Bearer {await self._token()}"},
                timeout=2.0,
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return ApiTokenInfo.from_json(response.json())
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise DependencyUnavailableError("projects") from exc


class FakeApiTokenReader:
    """Токены в памяти: секрет → описание."""

    def __init__(self, tokens: dict[str, ApiTokenInfo] | None = None) -> None:
        self.tokens = tokens or {}

    async def resolve(self, secret: str) -> ApiTokenInfo | None:
        return self.tokens.get(secret)
