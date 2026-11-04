import time
import uuid

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from aiip_kit.errors import NotAuthenticatedError, PermissionDeniedError
from aiip_kit.service_auth.fake import FakeServiceTokenProvider, FakeServiceTokenVerifier
from aiip_kit.service_auth.jwks import StaticKeys
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from aiip_kit.service_auth.verifier import JwtServiceTokenVerifier

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
VERIFIER = JwtServiceTokenVerifier(StaticKeys({"k1": KEY.public_key()}), audience="projects")


def sign(**claims: object) -> str:
    now = int(time.time())
    payload = {
        "iss": "auth",
        "aud": "projects",
        "sub": "incidents",
        "token_use": "service",
        "scope": "projects.acl.read",
        "iat": now,
        "exp": now + 300,
        "jti": str(uuid.uuid4()),
    } | claims
    return jwt.encode(payload, KEY, algorithm="RS256", headers={"kid": "k1"})


async def test_service_token_with_scope_passes() -> None:
    identity = await VERIFIER.verify(f"Bearer {sign()}", "projects.acl.read")

    assert identity.service == "incidents"
    assert identity.scopes == {"projects.acl.read"}


async def test_missing_scope_is_forbidden() -> None:
    with pytest.raises(PermissionDeniedError):
        await VERIFIER.verify(f"Bearer {sign()}", "projects.tokens.read")


@pytest.mark.parametrize(
    "claims",
    [
        {"token_use": None},  # пользовательский токен
        {"sid": "session"},
        {"aud": "users"},
        {"exp": int(time.time()) - 10},
    ],
)
async def test_not_a_service_token_for_us_is_rejected(claims: dict[str, object]) -> None:
    token = sign(**claims)

    with pytest.raises(NotAuthenticatedError):
        await VERIFIER.verify(f"Bearer {token}", "projects.acl.read")


async def test_provider_caches_token_per_audience_and_scopes() -> None:
    issued: list[dict[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        form = dict(httpx.QueryParams(request.content.decode()))
        issued.append(form)
        return httpx.Response(200, json={"access_token": f"t{len(issued)}", "expires_in": 300})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        provider = AuthServiceTokenProvider(client, "http://auth", "incidents", "secret")
        first = await provider.token("projects", ["projects.acl.read"])
        again = await provider.token("projects", ["projects.acl.read"])
        other = await provider.token("users", ["users.lookup"])

    assert first == again == "t1"
    assert other == "t2"
    assert issued[0] == {
        "grant_type": "client_credentials",
        "audience": "projects",
        "scope": "projects.acl.read",
    }


async def test_fake_pair_understands_each_other() -> None:
    token = await FakeServiceTokenProvider("incidents").token("projects", ["projects.acl.read"])
    verifier = FakeServiceTokenVerifier("projects")

    identity = await verifier.verify(f"Bearer {token}", "projects.acl.read")

    assert identity.service == "incidents"
    with pytest.raises(NotAuthenticatedError):
        await FakeServiceTokenVerifier("users").verify(f"Bearer {token}", "projects.acl.read")
