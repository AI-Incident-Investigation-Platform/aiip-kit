import json
import uuid
from datetime import UTC, datetime, timedelta

import httpx
from fakeredis import FakeAsyncRedis

from aiip_kit.api_tokens.reader import ApiTokenInfo, DragonflyApiTokenReader, token_digest

NOW = 1_790_000_000_000


def token_json(**fields: object) -> dict[str, object]:
    return {
        "state": "active",
        "token_id": str(uuid.uuid4()),
        "project_id": str(uuid.uuid4()),
        "environment_id": None,
        "scopes": ["alert.ingest"],
        "lifecycle_version": 1,
        "ver": 1,
        "expires_at": None,
        "observed_at_ms": NOW,
        "fresh_until_ms": NOW + 5000,
    } | fields


async def no_token() -> str:
    return "service-token"


async def test_fresh_cache_resolves_without_projects() -> None:
    redis = FakeAsyncRedis()
    value = token_json()
    await redis.set(f"projects:token:{token_digest('aiip_secret')}", json.dumps(value))
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        reader = DragonflyApiTokenReader(
            redis, client, "http://projects", no_token, clock_ms=lambda: NOW
        )
        info = await reader.resolve("aiip_secret")

    assert info is not None and info.project_id == uuid.UUID(str(value["project_id"]))
    assert calls == []


async def test_miss_asks_projects_and_unknown_is_none() -> None:
    redis = FakeAsyncRedis()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == "Bearer service-token"
        if request.url.path.endswith(token_digest("aiip_known")):
            return httpx.Response(200, json=token_json())
        return httpx.Response(404)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        reader = DragonflyApiTokenReader(
            redis, client, "http://projects", no_token, clock_ms=lambda: NOW
        )
        assert await reader.resolve("aiip_known") is not None
        assert await reader.resolve("aiip_unknown") is None


def test_scope_and_expiry_are_checked() -> None:
    now = datetime.now(UTC)
    info = ApiTokenInfo.from_json(token_json(expires_at=(now + timedelta(hours=1)).isoformat()))

    assert info.allows("alert.ingest", now)
    assert not info.allows("deployment.ingest", now)
    assert not info.allows("alert.ingest", now + timedelta(hours=2))
    assert not ApiTokenInfo.from_json(token_json(state="revoked")).allows("alert.ingest", now)
