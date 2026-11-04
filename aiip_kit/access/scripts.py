"""Lua-скрипты прав в Dragonfly. Имена всех ключей приходят в KEYS: глобальной транзакции нет.

Формат полей — `backend/decisions.md`, пункт 1.
"""

# KEYS: сессия auth, members проекта, roles проекта. ARGV: user_id, now_ms.
# Ответ: {статус} или {"ok", role_key, permissions_json, lifecycle_version}.
CHECK = """
if redis.call('EXISTS', KEYS[1]) == 0 then return {'no_session'} end
local project_raw = redis.call('HGET', KEYS[2], '$project')
local member_raw = redis.call('HGET', KEYS[2], ARGV[1])
if not project_raw or not member_raw then return {'miss'} end
local now = tonumber(ARGV[2])
local project = cjson.decode(project_raw)
local member = cjson.decode(member_raw)
if project.fresh_until_ms < now or member.fresh_until_ms < now then return {'stale'} end
if project.state ~= 'active' then return {'inactive'} end
if member.deleted then return {'not_member'} end
local role_raw = redis.call('HGET', KEYS[3], member.role_key)
if not role_raw then return {'miss'} end
local role = cjson.decode(role_raw)
if role.fresh_until_ms < now then return {'stale'} end
if role.deleted then return {'not_member'} end
return {'ok', member.role_key, cjson.encode(role.permissions), tostring(project.lifecycle_version)}
"""

# KEYS: members проекта, roles проекта.
# ARGV: user_id, project_json, member_json, role_key ('' — нет роли), role_json, ttl_seconds.
# Поле пишется, если версия больше или та же версия наблюдалась позже: старое чтение
# из Postgres не перетирает новое, а неизменившийся grant освежается.
STORE = """
local function newer(raw, candidate)
  if not raw then return true end
  local current = cjson.decode(raw)
  if candidate.ver > current.ver then return true end
  return candidate.ver == current.ver and candidate.observed_at_ms > current.observed_at_ms
end
local written = 0
local project = cjson.decode(ARGV[2])
if newer(redis.call('HGET', KEYS[1], '$project'), project) then
  redis.call('HSET', KEYS[1], '$project', ARGV[2]); written = written + 1
end
local member = cjson.decode(ARGV[3])
if newer(redis.call('HGET', KEYS[1], ARGV[1]), member) then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[3]); written = written + 1
end
if ARGV[4] ~= '' then
  local role = cjson.decode(ARGV[5])
  if newer(redis.call('HGET', KEYS[2], ARGV[4]), role) then
    redis.call('HSET', KEYS[2], ARGV[4], ARGV[5]); written = written + 1
  end
end
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[6]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[6]))
return written
"""
