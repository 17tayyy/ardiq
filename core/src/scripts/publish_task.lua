---@diagnostic disable: undefined-global
-- Stage a task atomically.
-- ARGV: 1 task-data key, 2 stream key, 3 delayed key, 4 task id, 5 payload,
--       6 score (0 = immediate), 7 expire ms (0 = none), 8 now ms.
-- Returns 1 if staged, 0 if a task with that id already existed.
local task_key = ARGV[1]
local stream   = ARGV[2]
local delayed  = ARGV[3]
local task_id  = ARGV[4]
local payload  = ARGV[5]
local score    = tonumber(ARGV[6])
local expire   = tonumber(ARGV[7])
local now      = ARGV[8]

local stored
if expire > 0 then
  stored = redis.call('SET', task_key, payload, 'NX', 'PX', expire)
else
  stored = redis.call('SET', task_key, payload, 'NX')
end
if not stored then return 0 end

if score > 0 then
  redis.call('ZADD', delayed, score, task_id)
else
  redis.call('XADD', stream, '*', 'task_id', task_id, 'enqueue_time', now)
end
return 1
