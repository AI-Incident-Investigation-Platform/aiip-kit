import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

import httpx

from aiip_kit.access.model import AclSnapshot
from aiip_kit.errors import DependencyUnavailableError

ACL_SCOPE = "projects.acl.read"


class AclSource(Protocol):
    async def fetch(self, project_id: UUID, user_id: UUID) -> AclSnapshot:
        """Авторитетные права из `projects`; `projects` сам кладёт их в Dragonfly.

        Raises:
            DependencyUnavailableError: `projects` недоступен или ответил не по контракту.
        """
        ...


class HttpAclSource:
    """`GET /internal/v1/acl` у `projects` со служебным токеном."""

    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        token: Callable[[], Awaitable[str]],
        timeout_seconds: float = 2.0,
    ) -> None:
        self._client = client
        self._url = f"{base_url.rstrip('/')}/internal/v1/acl"
        self._token = token
        self._timeout = timeout_seconds

    async def fetch(self, project_id: UUID, user_id: UUID) -> AclSnapshot:
        try:
            response = await self._client.get(
                self._url,
                params={"project_id": str(project_id), "user_id": str(user_id)},
                headers={"Authorization": f"Bearer {await self._token()}"},
                timeout=self._timeout,
            )
            response.raise_for_status()
            return AclSnapshot.from_json(response.json())
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            raise DependencyUnavailableError("projects") from exc


class FakeAclSource:
    """Источник для тестов читателя: отдаёт заранее заданные снимки и считает запросы."""

    def __init__(self, snapshots: dict[tuple[UUID, UUID], AclSnapshot] | None = None) -> None:
        self.snapshots = snapshots or {}
        self.calls = 0
        self.available = True

    async def fetch(self, project_id: UUID, user_id: UUID) -> AclSnapshot:
        self.calls += 1
        # как сетевой вызов: отдаёт управление, параллельные запросы успевают совпасть
        await asyncio.sleep(0)
        if not self.available:
            raise DependencyUnavailableError("projects")
        return self.snapshots[(project_id, user_id)]
