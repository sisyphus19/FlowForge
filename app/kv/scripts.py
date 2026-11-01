"""Lua scripts: every multi-key state transition is a single atomic Redis operation.

Ownership model
---------------
The authority on "who owns a running task" is the task hash: state == 'running' and
fence == <the fence handed out at claim time>. The lease key (with TTL) is only a *liveness
signal*: if it expires, recovery may take the task away, which bumps state/attempt and
invalidates the old fence. A slow worker that finishes before recovery runs may still
complete safely; a worker that finishes after takeover is rejected (fencing).
"""

PRELUDE = """
local function now_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end
local function f(n) return string.format('%.0f', n) end
"""

# KEYS: task, stream | ARGV: task_id, run_id
ENQUEUE = PRELUDE + """
if redis.call('HGET', KEYS[1], 'state') ~= 'pending' then return 0 end
local attempt = redis.call('HINCRBY', KEYS[1], 'attempt', 1)
redis.call('HSET', KEYS[1], 'state', 'queued', 'queued_ms', f(now_ms()))
redis.call('XADD', KEYS[2], '*', 'task_id', ARGV[1], 'run_id', ARGV[2], 'attempt', attempt)
return 1
"""

# KEYS: task, lease, fence, running | ARGV: task_id, worker_id, attempt, ttl_ms
CLAIM = PRELUDE + """
local h = redis.call('HMGET', KEYS[1], 'state', 'attempt')
if h[1] ~= 'queued' or h[2] ~= ARGV[3] then return {0} end
local fence = redis.call('INCR', KEYS[3])
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[4])
redis.call('HSET', KEYS[1], 'state', 'running', 'worker', ARGV[2], 'fence', fence, 'started_ms', f(now_ms()))
redis.call('SADD', KEYS[4], ARGV[1])
return {1, fence}
"""

# KEYS: task, lease | ARGV: worker_id, fence, ttl_ms   -> 0 lost, 1 ok, 2 ok+cancel requested
RENEW = PRELUDE + """
local h = redis.call('HMGET', KEYS[1], 'state', 'fence')
if h[1] ~= 'running' or h[2] ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[2], ARGV[1], 'PX', ARGV[3])
if redis.call('HGET', KEYS[1], 'cancel') == '1' then return 2 end
return 1
"""

# KEYS: task, lease, result, running | ARGV: fence, result_json, task_id  -> 0 rejected, 1 ok, 2 cancelled
COMPLETE = PRELUDE + """
local h = redis.call('HMGET', KEYS[1], 'state', 'fence')
if h[1] ~= 'running' or h[2] ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[4], ARGV[3])
if redis.call('HGET', KEYS[1], 'cancel') == '1' then
  redis.call('HSET', KEYS[1], 'state', 'cancelled', 'finished_ms', f(now_ms()))
  return 2
end
redis.call('SET', KEYS[3], ARGV[2])
redis.call('HSET', KEYS[1], 'state', 'succeeded', 'finished_ms', f(now_ms()))
return 1
"""

# KEYS: task, lease, running, retry, dead
# ARGV: fence, error, task_id, retryable('1'/'0'), delay_ms, run_id
# -> {0} rejected | {1,'retry_wait'} | {1,'failed'} | {1,'cancelled'}
FAIL = PRELUDE + """
local h = redis.call('HMGET', KEYS[1], 'state', 'fence')
if h[1] ~= 'running' or h[2] ~= ARGV[1] then return {0} end
local now = now_ms()
redis.call('DEL', KEYS[2])
redis.call('SREM', KEYS[3], ARGV[3])
local failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
redis.call('HSET', KEYS[1], 'last_error', ARGV[2])
if redis.call('HGET', KEYS[1], 'cancel') == '1' then
  redis.call('HSET', KEYS[1], 'state', 'cancelled', 'finished_ms', f(now))
  return {1, 'cancelled'}
end
local maxa = tonumber(redis.call('HGET', KEYS[1], 'max_attempts'))
if ARGV[4] == '1' and failures < maxa then
  redis.call('HSET', KEYS[1], 'state', 'retry_wait', 'retry_at_ms', f(now + tonumber(ARGV[5])))
  redis.call('ZADD', KEYS[4], now + tonumber(ARGV[5]), ARGV[3])
  return {1, 'retry_wait'}
end
redis.call('HSET', KEYS[1], 'state', 'failed', 'finished_ms', f(now))
redis.call('XADD', KEYS[5], '*', 'task_id', ARGV[3], 'run_id', ARGV[6], 'error', ARGV[2])
return {1, 'failed'}
"""

# KEYS: retry zset, stream | ARGV: limit, task_prefix, state_suffix  -> number promoted
PROMOTE_DUE = PRELUDE + """
local now = now_ms()
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
local n = 0
for _, tid in ipairs(due) do
  redis.call('ZREM', KEYS[1], tid)
  local tk = ARGV[2] .. tid .. ARGV[3]
  if redis.call('HGET', tk, 'state') == 'retry_wait' then
    local attempt = redis.call('HINCRBY', tk, 'attempt', 1)
    redis.call('HSET', tk, 'state', 'queued', 'queued_ms', f(now))
    redis.call('XADD', KEYS[2], '*', 'task_id', tid, 'run_id', redis.call('HGET', tk, 'run_id'), 'attempt', attempt)
    n = n + 1
  end
end
return n
"""

# Tasks whose lease key vanished (worker died / partitioned) are re-queued or dead-lettered.
# KEYS: running set, stream, dead stream | ARGV: limit, task_prefix, lease_suffix, state_suffix
# -> flat list: task_id, outcome ('requeued'|'failed'|'cancelled'), run_id, ...
REQUEUE_EXPIRED = PRELUDE + """
local ids = redis.call('SMEMBERS', KEYS[1])
local out = {}
local n = 0
local now = now_ms()
for _, tid in ipairs(ids) do
  if n >= tonumber(ARGV[1]) then break end
  local tk = ARGV[2] .. tid .. ARGV[4]
  local st = redis.call('HGET', tk, 'state')
  if st ~= 'running' then
    redis.call('SREM', KEYS[1], tid)
  elseif redis.call('EXISTS', ARGV[2] .. tid .. ARGV[3]) == 0 then
    n = n + 1
    local run_id = redis.call('HGET', tk, 'run_id')
    redis.call('SREM', KEYS[1], tid)
    local failures = redis.call('HINCRBY', tk, 'failures', 1)
    local maxa = tonumber(redis.call('HGET', tk, 'max_attempts'))
    redis.call('HSET', tk, 'last_error', 'lease expired (worker lost)')
    local outcome
    if redis.call('HGET', tk, 'cancel') == '1' then
      redis.call('HSET', tk, 'state', 'cancelled', 'finished_ms', f(now))
      outcome = 'cancelled'
    elseif failures < maxa then
      local attempt = redis.call('HINCRBY', tk, 'attempt', 1)
      redis.call('HSET', tk, 'state', 'queued', 'queued_ms', f(now))
      redis.call('XADD', KEYS[2], '*', 'task_id', tid, 'run_id', run_id, 'attempt', attempt)
      outcome = 'requeued'
    else
      redis.call('HSET', tk, 'state', 'failed', 'finished_ms', f(now))
      redis.call('XADD', KEYS[3], '*', 'task_id', tid, 'run_id', run_id, 'error', 'lease expired (worker lost)')
      outcome = 'failed'
    end
    out[#out + 1] = tid
    out[#out + 1] = outcome
    out[#out + 1] = run_id
  end
end
return out
"""

# KEYS: task, retry zset | ARGV: task_id -> resulting/current state
CANCEL_TASK = PRELUDE + """
local st = redis.call('HGET', KEYS[1], 'state')
if not st then return 'missing' end
if st == 'pending' or st == 'queued' or st == 'retry_wait' then
  redis.call('HSET', KEYS[1], 'state', 'cancelled', 'finished_ms', f(now_ms()))
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 'cancelled'
end
if st == 'running' then redis.call('HSET', KEYS[1], 'cancel', '1') end
return st
"""

# KEYS: task | ARGV: from_state, to_state -> 1 if moved
TRANSITION = PRELUDE + """
if redis.call('HGET', KEYS[1], 'state') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[2], 'finished_ms', f(now_ms()))
return 1
"""

# Dead-letter replay: failed -> pending with a fresh retry budget.  KEYS: task
REPLAY = PRELUDE + """
if redis.call('HGET', KEYS[1], 'state') ~= 'failed' then return 0 end
redis.call('HSET', KEYS[1], 'state', 'pending', 'failures', 0)
redis.call('HDEL', KEYS[1], 'finished_ms', 'last_error', 'cancel')
return 1
"""

# KEYS: run hash, runs:active, flow active | ARGV: final_state, run_id  -> 1 if this call finalized the run
RUN_FINALIZE = PRELUDE + """
if redis.call('HGET', KEYS[1], 'state') ~= 'running' then return 0 end
redis.call('HSET', KEYS[1], 'state', ARGV[1], 'finished_ms', f(now_ms()))
redis.call('SREM', KEYS[2], ARGV[2])
redis.call('SREM', KEYS[3], ARGV[2])
return 1
"""

# KEYS: run hash, runs:active, flow active | ARGV: run_id -> 1 if reopened (only failed runs can reopen)
RUN_REOPEN = PRELUDE + """
local st = redis.call('HGET', KEYS[1], 'state')
if st == 'running' then return 1 end
if st ~= 'failed' then return 0 end
redis.call('HSET', KEYS[1], 'state', 'running')
redis.call('HDEL', KEYS[1], 'finished_ms')
redis.call('SADD', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
return 1
"""

# Concurrency policy slot. KEYS: flow active set | ARGV: run_id, max_runs, enforce('1'/'0')
ACQUIRE_SLOT = """
if ARGV[3] == '1' and redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('SADD', KEYS[1], ARGV[1])
return 1
"""
