from dataclasses import dataclass
from typing import Protocol

import jwt

from aiip_kit.errors import NotAuthenticatedError, PermissionDeniedError
from aiip_kit.service_auth.jwks import KeyResolver

SERVICE_TOKEN_USE = "service"


@dataclass(frozen=True)
class ServiceIdentity:
    """Сервис, вызвавший internal API."""

    service: str
    scopes: frozenset[str]


class ServiceTokenVerifier(Protocol):
    async def verify(self, authorization: str | None, scope: str) -> ServiceIdentity:
        """Проверяет служебный токен из `Authorization: Bearer` и право на эндпоинт.

        Raises:
            NotAuthenticatedError: токена нет, он не служебный или не прошёл проверку — 401.
            PermissionDeniedError: у сервиса нет `scope` — 403.
        """
        ...


def bearer(authorization: str | None) -> str | None:
    scheme, _, token = (authorization or "").partition(" ")
    return token if scheme.lower() == "bearer" and token else None


def require_scope(identity: ServiceIdentity, scope: str) -> ServiceIdentity:
    if scope not in identity.scopes:
        raise PermissionDeniedError(scope)
    return identity


class JwtServiceTokenVerifier:
    """Служебный JWT `auth`: RS256, `iss`, своя `aud`, `token_use=service`, без `sid`.

    Пользовательский токен сюда не подходит: у него нет `token_use=service`.
    """

    def __init__(self, keys: KeyResolver, audience: str, issuer: str = "auth") -> None:
        self._keys = keys
        self._audience = audience
        self._issuer = issuer

    async def verify(self, authorization: str | None, scope: str) -> ServiceIdentity:
        token = bearer(authorization)
        if token is None:
            raise NotAuthenticatedError
        try:
            kid = jwt.get_unverified_header(token).get("kid")
        except jwt.PyJWTError as exc:
            raise NotAuthenticatedError from exc
        if not isinstance(kid, str) or (key := await self._keys.get(kid)) is None:
            raise NotAuthenticatedError
        try:
            claims = jwt.decode(
                token,
                key,
                algorithms=["RS256"],
                issuer=self._issuer,
                audience=self._audience,
                options={"require": ["iss", "aud", "sub", "iat", "exp", "jti"]},
            )
        except jwt.PyJWTError as exc:
            raise NotAuthenticatedError from exc
        if claims.get("token_use") != SERVICE_TOKEN_USE or "sid" in claims:
            raise NotAuthenticatedError
        identity = ServiceIdentity(
            service=str(claims["sub"]),
            scopes=frozenset(str(claims.get("scope", "")).split()),
        )
        return require_scope(identity, scope)
