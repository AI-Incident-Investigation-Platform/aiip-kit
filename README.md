# aiip-kit

Общий код сервисов AIIP: то, что должно работать одинаково в каждом
сервисе, — конверт сообщений, outbox и inbox, консьюмер с повторами,
проверка прав проекта, служебные токены, API-токены ingest. Контракты,
которые он реализует, — `backend/decisions.md` и `backend/messaging.md` в
рабочей папке.

У каждой части есть **fake** с тем же интерфейсом: права, токены и
публикация задаются в памяти. С ними сервис работает и тестируется без
`projects`, `auth`, Dragonfly и RabbitMQ — пока соседей нет и в unit-тестах.
Переход на настоящую реализацию — замена одного объекта при сборке
зависимостей, код сервиса не меняется.

Чего здесь нет: бизнес-моделей, `Permission`, DI, настроек и миграций —
это у сервиса. Код пакета никогда сам не делает commit.

## Подключение

В `pyproject.toml` сервиса — по тегу, без git в образе:

```toml
dependencies = [
    "aiip-kit @ https://github.com/AI-Incident-Investigation-Platform/aiip-kit/archive/refs/tags/v0.1.0.tar.gz",
]
```

Новая версия: здесь `make release V=0.1.1`, в сервисе — поменять тег и
`uv lock`. Все сервисы обновлять одновременно не нужно.

## Что внутри

| Модуль | Что | Fake |
|---|---|---|
| `messaging.message` | `MessageV1` — конверт события и команды | — |
| `messaging.topology` | имена exchange'ей, очередей, live-адресов; `Destination` | — |
| `messaging.publisher` | `AmqpPublisher` с publisher confirm | `FakePublisher` |
| `messaging.consumer` | `Consumer`: очередь, retry 5/30/300 с, dead 14 дней | — |
| `outbox.tables` | таблицы `outbox` и `inbox` в метаданных сервиса | — |
| `outbox.writer` | `OutboxWriter.add` в транзакции сервиса | `FakeOutboxWriter` |
| `outbox.relay` | `OutboxRelay`: lease, `SKIP LOCKED`, повторы, уборка | — |
| `outbox.inbox` | `InboxGuard.accept`: повторная доставка ничего не делает | — |
| `access.checker` | `DragonflyAccessChecker`: сессия и права одним Lua; `AclCacheWriter` — только для `projects` | `FakeAccessChecker` |
| `access.source` | `HttpAclSource` — `GET projects /internal/v1/acl` | `FakeAclSource` |
| `service_auth` | `AuthServiceTokenProvider`, `JwtServiceTokenVerifier`, `JwksKeys` | `FakeServiceTokenProvider`, `FakeServiceTokenVerifier` |
| `api_tokens.reader` | `DragonflyApiTokenReader` — `X-Ingest-Key` → проект | `FakeApiTokenReader` |
| `errors` | ошибки, которые сервис переводит в свой ответ | — |

Ошибки → HTTP: `NotAuthenticatedError` 401, `PermissionDeniedError` 403,
`ProjectNotFoundError` 404, `DependencyUnavailableError` 503.

## Как пользоваться

### Таблицы outbox и inbox

```python
# src/core/db/models/messaging.py
from aiip_kit.outbox.tables import inbox_table, outbox_table
from src.core.db.models.base import Base

outbox = outbox_table(Base.metadata)
inbox = inbox_table(Base.metadata)
```

Миграцию создаёт `make migrate` сервиса, как для любой модели.

### Событие вместе с изменением

```python
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.writer import OutboxWriter

writer = OutboxWriter(outbox)

async with session.begin():
    incident = await repo.create(...)
    await writer.add(
        session,
        MessageV1.event(
            "incident.created",
            "incidents",
            {"incident": incident_json},
            project_id=incident.project_id,
            aggregate_type="incident",
            aggregate_id=incident.id,
            aggregate_version=incident.version,
            actor=Actor(kind="user", id=str(user_id)),
        ),
        Destination.event("incidents", "incident.created"),
    )
```

Relay — фоновая задача в процессе сервиса:

```python
from aiip_kit.messaging.publisher import AmqpPublisher
from aiip_kit.outbox.relay import OutboxRelay

relay = OutboxRelay(session_maker, outbox, AmqpPublisher(cfg.rabbitmq.url))
task = asyncio.create_task(relay.run(stop_event, listen_dsn=cfg.database.dsn))
```

`listen_dsn` — одно соединение `LISTEN` на под для мгновенной отправки;
без него relay опрашивает таблицу раз в секунду.

### Консьюмер

```python
from aiip_kit.errors import TransientError
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec
from aiip_kit.messaging.topology import events_exchange, events_queue
from aiip_kit.outbox.inbox import InboxGuard

guard = InboxGuard(inbox)


async def on_incident_created(message: MessageV1, attempt: int) -> None:
    try:
        async with session_maker() as session, session.begin():
            if not await guard.accept(session, "notifications.incident_created", message):
                return  # уже обработано
            await notify_members(session, message)
    except (DbUnavailableError, CacheUnavailableError) as exc:
        raise TransientError from exc  # повтор через 5/30/300 с


spec = QueueSpec(
    name=events_queue("notifications", "incidents"),
    bindings=[Binding(events_exchange("incidents"), "topic", "incident.created")],
)
channel = await connection.channel(publisher_confirms=True)
await Consumer(channel, spec, on_incident_created).start()
```

### Права в эндпоинте

```python
from aiip_kit.access.checker import DragonflyAccessChecker, FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.access.source import HttpAclSource

# настоящая
checker = DragonflyAccessChecker(
    redis, HttpAclSource(http, cfg.projects.url, lambda: tokens.token("projects", ["projects.acl.read"]))
)
# пока нет projects или в тестах
checker = FakeAccessChecker()
checker.allow(project_id, user_id, "project.read", "incident.manage")

grant = await checker.require(Principal(user_id, session_id), project_id, "incident.manage")
```

`Principal` — из уже проверенного access-токена (`sub`, `sid`), как в
`SessionAuthenticator` сервисов.

### Служебные токены

```python
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from aiip_kit.service_auth.fake import FakeServiceTokenProvider, FakeServiceTokenVerifier
from aiip_kit.service_auth.jwks import JwksKeys
from aiip_kit.service_auth.verifier import JwtServiceTokenVerifier

# вызвать чужой internal API
tokens = AuthServiceTokenProvider(http, cfg.auth.url, cfg.service.client_id, cfg.service.client_secret)
token = await tokens.token("projects", ["projects.acl.read"])

# принять вызов своего internal API
verifier = JwtServiceTokenVerifier(JwksKeys(http, cfg.auth.jwks_url), audience="incidents")
caller = await verifier.verify(request.headers.get("Authorization"), "incidents.ai.events")

# пока в auth нет выдачи — пара fake, понимающих друг друга
tokens = FakeServiceTokenProvider("incidents")
verifier = FakeServiceTokenVerifier("incidents")
```

### API-токен ingest

```python
from aiip_kit.api_tokens.reader import DragonflyApiTokenReader, FakeApiTokenReader

reader = DragonflyApiTokenReader(redis, http, cfg.projects.url,
                                 lambda: tokens.token("projects", ["projects.tokens.read"]))
info = await reader.resolve(request.headers["X-Ingest-Key"])
if info is None or not info.allows("alert.ingest", now):
    raise InvalidIngestKeyError
```

## Проверки

```bash
make check              # ruff format --check + ruff check + mypy
make test               # unit, без Docker; Lua-скрипты — на fakeredis
make test-integration   # outbox, relay, inbox на Postgres в Testcontainers, нужен Docker
```

Устройство и решения — [docs/design.md](docs/design.md).
