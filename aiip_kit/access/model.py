from dataclasses import dataclass
from typing import Any, Literal, Self
from uuid import UUID

ProjectState = Literal["active", "deleting", "missing"]

FRESHNESS_MS = 5_000


@dataclass(frozen=True)
class Principal:
    """Пользователь запроса: из проверенного access-токена."""

    user_id: UUID
    session_id: UUID


@dataclass(frozen=True)
class ProjectGrant:
    """Права пользователя в проекте на момент проверки."""

    project_id: UUID
    user_id: UUID
    role_key: str
    permissions: frozenset[str]
    lifecycle_version: int

    def has(self, permission: str) -> bool:
        return permission in self.permissions


@dataclass(frozen=True)
class AclSnapshot:
    """Ответ `GET projects /internal/v1/acl` (`backend/decisions.md`, пункт 1).

    `member` и `role` — `None`, если пользователь не участник: это тоже
    ответ, его кэшируют.
    """

    project_id: UUID
    user_id: UUID
    project_state: ProjectState
    lifecycle_version: int
    acl_version: int
    member_role_key: str | None
    member_version: int | None
    role_key: str | None
    role_version: int | None
    permissions: frozenset[str]
    observed_at_ms: int
    fresh_until_ms: int

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> Self:
        member = value.get("member")
        role = value.get("role")
        return cls(
            project_id=UUID(value["project_id"]),
            user_id=UUID(value["user_id"]),
            project_state=value["project_state"],
            lifecycle_version=int(value["lifecycle_version"]),
            acl_version=int(value["acl_version"]),
            member_role_key=member["role_key"] if member else None,
            member_version=int(member["version"]) if member else None,
            role_key=role["key"] if role else None,
            role_version=int(role["version"]) if role else None,
            permissions=frozenset(role["permissions"]) if role else frozenset(),
            observed_at_ms=int(value["observed_at_ms"]),
            fresh_until_ms=int(value["fresh_until_ms"]),
        )

    def to_json(self) -> dict[str, Any]:
        return {
            "project_id": str(self.project_id),
            "user_id": str(self.user_id),
            "project_state": self.project_state,
            "lifecycle_version": self.lifecycle_version,
            "acl_version": self.acl_version,
            "member": (
                {"role_key": self.member_role_key, "version": self.member_version}
                if self.member_role_key is not None
                else None
            ),
            "role": (
                {
                    "key": self.role_key,
                    "version": self.role_version,
                    "permissions": sorted(self.permissions),
                }
                if self.role_key is not None
                else None
            ),
            "observed_at_ms": self.observed_at_ms,
            "fresh_until_ms": self.fresh_until_ms,
        }

    def grant(self) -> ProjectGrant | None:
        """Права, если проект активен и пользователь в нём участник; иначе `None`."""
        if self.project_state != "active" or self.member_role_key is None:
            return None
        return ProjectGrant(
            project_id=self.project_id,
            user_id=self.user_id,
            role_key=self.member_role_key,
            permissions=self.permissions,
            lifecycle_version=self.lifecycle_version,
        )
