.PHONY: help sync format lint lint-fix typecheck check test test-integration test-all release

SRC := aiip_kit tests

help:
	@echo ""
	@echo "  aiip-kit — общий код сервисов, подключается зависимостью по тегу"
	@echo ""
	@echo "  Качество кода"
	@echo "    sync                      поставить зависимости в .venv"
	@echo "    check                     формат + линт + типы"
	@echo "    format                    форматирование ruff"
	@echo "    lint                      проверка линтером"
	@echo "    lint-fix                  форматирование + автоисправление линтера"
	@echo "    typecheck                 проверка типов mypy"
	@echo ""
	@echo "  Тесты"
	@echo "    test                      unit-тесты (без Docker; Lua — на fakeredis)"
	@echo "    test-integration          integration-тесты (Postgres в Testcontainers, нужен Docker)"
	@echo "    test-all                  все тесты"
	@echo ""
	@echo "  Выпуск"
	@echo "    release V=0.1.1           поставить версию в pyproject.toml, тег v<V> и запушить"
	@echo ""

sync:
	uv sync

format:
	uv run ruff format $(SRC)

lint:
	uv run ruff check $(SRC)

lint-fix:
	uv run ruff format $(SRC)
	uv run ruff check --fix $(SRC)

typecheck:
	uv run mypy

check:
	uv run ruff format --check $(SRC)
	uv run ruff check $(SRC)
	uv run mypy

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest tests/integration

test-all:
	uv run pytest

release:
	@test -n "$(V)" || { echo "нужна версия: make release V=0.1.1"; exit 1; }
	@git diff --quiet && git diff --cached --quiet || { echo "есть незакоммиченные изменения"; exit 1; }
	sed -i '' 's/^version = ".*"/version = "$(V)"/' pyproject.toml
	uv lock
	git commit -am "build: выпустил $(V)"
	git tag v$(V)
	git push && git push origin v$(V)
