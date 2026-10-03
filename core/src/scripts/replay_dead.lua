---@diagnostic disable: undefined-global
-- Move a dead task back into its stream atomically, so a replay that dies again
-- before this returns can never have its new dead entry deleted by this one.
-- ARGV: 1 dead key, 2 dead index, 3 task-data key, 4 stream key, 5 result key,
--       6 results index, 7 task id, 8 payload, 9 now ms.
-- Returns 1 if replayed, 0 if no dead task has that id.
local dead_key      = ARGV[1]
local dead_index    = ARGV[2]
local task_key      = ARGV[3]
local stream        = ARGV[4]
local result_key    = ARGV[5]
local results_index = ARGV[6]
local task_id       = ARGV[7]
local payload       = ARGV[8]
local now           = ARGV[9]

if redis.call('EXISTS', dead_key) == 0 then return 0 end
redis.call('DEL', dead_key)
redis.call('ZREM', dead_index, task_id)

-- A task with this id already queued (a unique call made again) covers it.
if not redis.call('SET', task_key, payload, 'NX') then return 1 end
redis.call('DEL', result_key)
redis.call('ZREM', results_index, task_id)
redis.call('XADD', stream, '*', 'task_id', task_id, 'enqueue_time', now)
return 1
