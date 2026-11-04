import asyncio
import uuid
from collections.abc import AsyncIterator
from uuid import UUID

import pytest
from fakeredis import FakeAsyncRedis

from aiip_kit.access.checker import (
    AclCacheWriter,
    DragonflyAccessChecker,
    FakeAccessChecker,
)
from aiip_kit.access.model import AclSnapshot, Principal
from aiip_kit.access.source import FakeAclSource
from aiip_kit.errors import (
    DependencyUnavailableError,
    NotAuthenticatedError,
    PermissionDeniedError,
    ProjectNotFoundError,
)

NOW = 1_790_000_000_000


@pytest.fixture
async def redis() -> AsyncIterator[FakeAsyncRedis]:
    client = FakeAsyncRedis()
    yield client
    await client.aclose()


def snapshot(
    project_id: UUID,
    user_id: UUID,
    *,
    role: str | None = "developer",
    permissions: tuple[str, ...] = ("project.read", "incident.manage"),
    acl_version: int = 1,
    observed_at_ms: int = NOW,
    state: str = "active",
) -> AclSnapshot:
    return AclSnapshot.from_json(
        {
            "project_id": str(project_id),
            "user_id": str(user_id),
            "project_state": state,
            "lifecycle_version": 1,
            "acl_version": acl_version,
            "member": {"role_key": role, "version": acl_version} if role else None,
            "role": {"key": role, "version": 1, "permissions": list(permissions)} if role else None,
            "observed_at_ms": observed_at_ms,
            "fresh_until_ms": observed_at_ms + 5000,
        }
    )


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> int:
        return self.now


async def live_session(redis: FakeAsyncRedis, principal: Principal) -> None:
    await redis.set(f"auth:session:{principal.session_id}", 1)


async def test_fresh_cache_answers_without_projects(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id))
    source = FakeAclSource()
    checker = DragonflyAccessChecker(redis, source, clock_ms=Clock())

    grant = await checker.require(principal, project_id, "incident.manage")

    assert grant.role_key == "developer"
    assert grant.permissions == {"project.read", "incident.manage"}
    assert source.calls == 0


async def test_missing_permission_is_denied(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id))
    checker = DragonflyAccessChecker(redis, FakeAclSource(), clock_ms=Clock())

    with pytest.raises(PermissionDeniedError) as denied:
        await checker.require(principal, project_id, "member.manage")

    assert denied.value.permission == "member.manage"


async def test_closed_session_is_not_authenticated(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id))
    checker = DragonflyAccessChecker(redis, FakeAclSource(), clock_ms=Clock())

    with pytest.raises(NotAuthenticatedError):
        await checker.grant(principal, project_id)


async def test_stale_cache_asks_projects_once_for_parallel_requests(
    redis: FakeAsyncRedis,
) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id))
    clock = Clock()
    clock.now = NOW + 6000
    source = FakeAclSource(
        {(project_id, principal.user_id): snapshot(project_id, principal.user_id)}
    )
    checker = DragonflyAccessChecker(redis, source, clock_ms=clock)

    grants = await asyncio.gather(*(checker.grant(principal, project_id) for _ in range(5)))

    assert {grant.role_key for grant in grants} == {"developer"}
    assert source.calls == 1


async def test_stale_cache_without_projects_is_refused(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id))
    clock = Clock()
    clock.now = NOW + 6000
    source = FakeAclSource()
    source.available = False
    checker = DragonflyAccessChecker(redis, source, clock_ms=clock)

    with pytest.raises(DependencyUnavailableError):
        await checker.grant(principal, project_id)


async def test_removed_member_gets_not_found(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    writer = AclCacheWriter(redis)
    await writer.store(snapshot(project_id, principal.user_id))
    await writer.store(
        snapshot(project_id, principal.user_id, role=None, acl_version=2, observed_at_ms=NOW + 10)
    )
    checker = DragonflyAccessChecker(redis, FakeAclSource(), clock_ms=Clock())

    with pytest.raises(ProjectNotFoundError):
        await checker.grant(principal, project_id)


async def test_old_read_does_not_restore_removed_access(redis: FakeAsyncRedis) -> None:
    project_id, user_id = uuid.uuid4(), uuid.uuid4()
    writer = AclCacheWriter(redis)
    await writer.store(snapshot(project_id, user_id, role=None, acl_version=2))

    written = await writer.store(
        snapshot(project_id, user_id, acl_version=1, observed_at_ms=NOW + 1)
    )

    member = await redis.hget(f"projects:members:{project_id}", str(user_id))
    assert member is not None and b'"deleted": true' in member
    assert written == 1  # только роль: членство и проект старее записанных


async def test_deleting_project_is_not_found(redis: FakeAsyncRedis) -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    await live_session(redis, principal)
    await AclCacheWriter(redis).store(snapshot(project_id, principal.user_id, state="deleting"))
    checker = DragonflyAccessChecker(redis, FakeAclSource(), clock_ms=Clock())

    with pytest.raises(ProjectNotFoundError):
        await checker.grant(principal, project_id)


async def test_fake_checker_follows_same_rules() -> None:
    project_id, principal = uuid.uuid4(), Principal(uuid.uuid4(), uuid.uuid4())
    checker = FakeAccessChecker()
    checker.allow(project_id, principal.user_id, "project.read")

    assert (await checker.require(principal, project_id, "project.read")).has("project.read")
    with pytest.raises(PermissionDeniedError):
        await checker.require(principal, project_id, "incident.manage")
    checker.remove(project_id, principal.user_id)
    with pytest.raises(ProjectNotFoundError):
        await checker.grant(principal, project_id)
