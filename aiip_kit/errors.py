class KitError(Exception):
    """Ошибка общего кода. Сервис переводит её в свой конверт ошибки и HTTP-статус."""


class NotAuthenticatedError(KitError):
    """Токена нет, он не прошёл проверку или сессия закрыта — 401."""


class ProjectNotFoundError(KitError):
    """Проекта нет, он удаляется или пользователь в нём не участник — 404."""

    def __init__(self, project_id: object) -> None:
        super().__init__(f"project {project_id} not found")
        self.project_id = project_id


class PermissionDeniedError(KitError):
    """Участник или сервис без нужного права — 403."""

    def __init__(self, permission: str) -> None:
        super().__init__(f"permission {permission} denied")
        self.permission = permission


class DependencyUnavailableError(KitError):
    """Dragonfly, projects, auth или брокер недоступны, решения принять нельзя — 503."""

    def __init__(self, dependency: str) -> None:
        super().__init__(f"{dependency} unavailable")
        self.dependency = dependency


class InboxConflictError(KitError):
    """Сообщение с уже принятым `message_id` пришло с другим телом: в dead, не обрабатывать."""


class TransientError(KitError):
    """Временный отказ при обработке сообщения: консьюмер повторит его позже."""
