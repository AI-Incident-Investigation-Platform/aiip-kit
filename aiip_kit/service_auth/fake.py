from collections.abc import Collection

from aiip_kit.errors import NotAuthenticatedError
from aiip_kit.service_auth.verifier import ServiceIdentity, bearer, require_scope

FAKE_PREFIX = "fake-service-token"


class FakeServiceTokenProvider:
    """Токен вида `fake-service-token:<сервис>:<aud>:<scopes>`; его понимает только fake-проверка.

    Для тестов и для работы, пока в `auth` нет выдачи служебных токенов.
    """

    def __init__(self, service: str) -> None:
        self._service = service

    async def token(self, audience: str, scopes: Collection[str]) -> str:
        return f"{FAKE_PREFIX}:{self._service}:{audience}:{','.join(sorted(scopes))}"


class FakeServiceTokenVerifier:
    """Принимает токены `FakeServiceTokenProvider` с нужной `aud`; scopes — из токена."""

    def __init__(self, audience: str) -> None:
        self._audience = audience

    async def verify(self, authorization: str | None, scope: str) -> ServiceIdentity:
        token = bearer(authorization)
        if token is None:
            raise NotAuthenticatedError
        prefix, _, rest = token.partition(":")
        service, _, rest = rest.partition(":")
        audience, _, scopes = rest.partition(":")
        if prefix != FAKE_PREFIX or not service or audience != self._audience:
            raise NotAuthenticatedError
        identity = ServiceIdentity(
            service=service, scopes=frozenset(filter(None, scopes.split(",")))
        )
        return require_scope(identity, scope)
