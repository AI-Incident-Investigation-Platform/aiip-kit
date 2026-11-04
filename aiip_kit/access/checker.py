import asyncio
import json
import time
from collections.abc import Callable
from typing import Protocol
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from aiip_kit.access.model import AclSnapshot, Principal, ProjectGrant
from aiip_kit.access.scripts import CHECK, STORE
from aiip_kit.access.source import AclSource
from aiip_kit.errors import (
    DependencyUnavailableError,
    NotAuthenticatedError,
    PermissionDeniedError,
    ProjectNotFoundError,
)

SESSION_PREFIX = "auth:session:"
MEMBERS_PREFIX = "projects:members:"
ROLES_PREFIX = "projects:roles:"
CACHE_TTL_SECONDS = 600


class AccessChecker(Protocol):
    async def grant(self, principal: Principal, project_id: UUID) -> ProjectGrant:
        """Права участника.

        Raises:
            NotAuthenticatedError: сессия закрыта — 401.
            ProjectNotFoundError: не участник, проекта нет или он удаляется — 404.
            DependencyUnavailableError: решения нет: Dragonfly или projects недоступны — 503.
        """
        ...

    async def require(
        self, principal: Principal, project_id: UUID, permission: str
    ) -> ProjectGrant:
        """Как `grant`, плюс нужное право.

        Raises:
            PermissionDeniedError: участник без права — 403.
        """
        ...


def _require(grant: ProjectGrant, permission: str) -> ProjectGrant:
    if not grant.has(permission):
        raise PermissionDeniedError(permission)
    return grant


class DragonflyAccessChecker:
    """Сессия и права одним Lua-запросом; промах или устаревание — `projects` через `source`.

    Одновременные промахи по одной паре проект–пользователь идут в `projects`
    одним запросом. Устаревшие данные без ответа `projects` — отказ, а не
    пропуск.
    """

    def __init__(
        self,
        redis: Redis,
        source: AclSource,
        *,
        session_prefix: str = SESSION_PREFIX,
        members_prefix: str = MEMBERS_PREFIX,
        roles_prefix: str = ROLES_PREFIX,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._redis = redis
        self._source = source
        self._session_prefix = session_prefix
        self._members_prefix = members_prefix
        self._roles_prefix = roles_prefix
        self._clock_ms = clock_ms
        self._check = redis.register_script(CHECK)
        self._inflight: dict[tuple[UUID, UUID], asyncio.Future[AclSnapshot]] = {}

    async def grant(self, principal: Principal, project_id: UUID) -> ProjectGrant:
        keys = [
            f"{self._session_prefix}{principal.session_id}",
            f"{self._members_prefix}{project_id}",
            f"{self._roles_prefix}{project_id}",
        ]
        try:
            reply = await self._check(keys=keys, args=[str(principal.user_id), self._clock_ms()])
        except RedisError as exc:
            raise DependencyUnavailableError("dragonfly") from exc
        status = _text(reply[0])
        if status == "no_session":
            raise NotAuthenticatedError
        if status in ("inactive", "not_member"):
            raise ProjectNotFoundError(project_id)
        if status == "ok":
            return ProjectGrant(
                project_id=project_id,
                user_id=principal.user_id,
                role_key=_text(reply[1]),
                permissions=frozenset(json.loads(_text(reply[2]))),
                lifecycle_version=int(_text(reply[3])),
            )
        snapshot = await self._fetch(project_id, principal.user_id)
        if (grant := snapshot.grant()) is None:
            raise ProjectNotFoundError(project_id)
        return grant

    async def require(
        self, principal: Principal, project_id: UUID, permission: str
    ) -> ProjectGrant:
        return _require(await self.grant(principal, project_id), permission)

    async def _fetch(self, project_id: UUID, user_id: UUID) -> AclSnapshot:
        key = (project_id, user_id)
        if (pending := self._inflight.get(key)) is not None:
            return await asyncio.shield(pending)
        future: asyncio.Future[AclSnapshot] = asyncio.get_running_loop().create_future()
        self._inflight[key] = future
        try:
            snapshot = await self._source.fetch(project_id, user_id)
        except BaseException as exc:
            future.set_exception(exc)
            # исключение уже отдано ожидающим; без этого asyncio ругается на непрочитанное
            future.exception()
            raise
        else:
            future.set_result(snapshot)
            return snapshot
        finally:
            del self._inflight[key]


class AclCacheWriter:
    """Запись прав в Dragonfly. Только для `projects`: остальные префикс `projects:*` читают."""

    def __init__(
        self,
        redis: Redis,
        *,
        members_prefix: str = MEMBERS_PREFIX,
        roles_prefix: str = ROLES_PREFIX,
        ttl_seconds: int = CACHE_TTL_SECONDS,
    ) -> None:
        self._members_prefix = members_prefix
        self._roles_prefix = roles_prefix
        self._ttl_seconds = ttl_seconds
        self._store = redis.register_script(STORE)

    async def store(self, snapshot: AclSnapshot) -> int:
        """Пишет состояние проекта, членство и роль, если они не старее записанных.

        Returns:
            Сколько полей записано: 0 — в кэше уже более новое.
        """
        seen = {
            "observed_at_ms": snapshot.observed_at_ms,
            "fresh_until_ms": snapshot.fresh_until_ms,
        }
        project = {
            "state": snapshot.project_state,
            "lifecycle_version": snapshot.lifecycle_version,
            "ver": snapshot.acl_version,
            **seen,
        }
        member = {
            "role_key": snapshot.member_role_key,
            "ver": snapshot.acl_version,
            "deleted": snapshot.member_role_key is None,
            **seen,
        }
        role = (
            {
                "permissions": sorted(snapshot.permissions),
                "ver": snapshot.role_version or 0,
                "deleted": False,
                **seen,
            }
            if snapshot.role_key is not None
            else None
        )
        try:
            written = await self._store(
                keys=[
                    f"{self._members_prefix}{snapshot.project_id}",
                    f"{self._roles_prefix}{snapshot.project_id}",
                ],
                args=[
                    str(snapshot.user_id),
                    json.dumps(project),
                    json.dumps(member),
                    snapshot.role_key or "",
                    json.dumps(role) if role is not None else "",
                    self._ttl_seconds,
                ],
            )
        except RedisError as exc:
            raise DependencyUnavailableError("dragonfly") from exc
        return int(written)


class FakeAccessChecker:
    """Права в памяти для тестов и для работы, пока нет `projects`.

    `grants` — проект → пользователь → набор прав. Закрытые сессии — в
    `revoked_sessions`, проекты в удалении — в `deleting`.
    """

    def __init__(self, grants: dict[UUID, dict[UUID, set[str]]] | None = None) -> None:
        self.grants: dict[UUID, dict[UUID, set[str]]] = grants or {}
        self.revoked_sessions: set[UUID] = set()
        self.deleting: set[UUID] = set()
        self.available = True

    def allow(self, project_id: UUID, user_id: UUID, *permissions: str) -> None:
        self.grants.setdefault(project_id, {}).setdefault(user_id, set()).update(permissions)

    def remove(self, project_id: UUID, user_id: UUID) -> None:
        self.grants.get(project_id, {}).pop(user_id, None)

    async def grant(self, principal: Principal, project_id: UUID) -> ProjectGrant:
        if not self.available:
            raise DependencyUnavailableError("dragonfly")
        if principal.session_id in self.revoked_sessions:
            raise NotAuthenticatedError
        permissions = self.grants.get(project_id, {}).get(principal.user_id)
        if permissions is None or project_id in self.deleting:
            raise ProjectNotFoundError(project_id)
        return ProjectGrant(
            project_id=project_id,
            user_id=principal.user_id,
            role_key="fake",
            permissions=frozenset(permissions),
            lifecycle_version=1,
        )

    async def require(
        self, principal: Principal, project_id: UUID, permission: str
    ) -> ProjectGrant:
        return _require(await self.grant(principal, project_id), permission)


def _text(value: bytes | str) -> str:
    return value.decode() if isinstance(value, bytes) else value
