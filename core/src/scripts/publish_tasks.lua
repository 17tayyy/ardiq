---@diagnostic disable: undefined-global
-- Stage a batch of tasks atomically, in one round trip.
-- ARGV: 1 now ms, 2 results index, then 9 fields per task: task-data key,
--       stream key, delayed key, task id, payload, score (0 = immediate),
--       expire ms (0 = none), result key, reset (1 = drop any stored result).
-- Returns one 1/0 per task, in order: 1 if staged, 0 if the id already existed.
local now           = ARGV[1]
local results_index = ARGV[2]
local staged = {}
local i = 3

while i <= #ARGV do
  local task_key   = ARGV[i]
  local stream     = ARGV[i + 1]
  local delayed    = ARGV[i + 2]
  local task_id    = ARGV[i + 3]
  local payload    = ARGV[i + 4]
  local score      = tonumber(ARGV[i + 5])
  local expire     = tonumber(ARGV[i + 6])
  local result_key = ARGV[i + 7]
  local reset      = tonumber(ARGV[i + 8])

  local stored
  if expire > 0 then
    stored = redis.call('SET', task_key, payload, 'NX', 'PX', expire)
  else
    stored = redis.call('SET', task_key, payload, 'NX')
  end

  if not stored then
    staged[#staged + 1] = 0
  else
    if reset == 1 then
      redis.call('DEL', result_key)
      redis.call('ZREM', results_index, task_id)
    end
    if score > 0 then
      redis.call('ZADD', delayed, score, task_id)
    else
      redis.call('XADD', stream, '*', 'task_id', task_id, 'enqueue_time', now)
    end
    staged[#staged + 1] = 1
  end

  i = i + 9
end

return staged
