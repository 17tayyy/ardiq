---@diagnostic disable: undefined-global
-- Abort a task atomically: mark it so a worker skips it, wake any worker already
-- running it, and finalize it here if it is only waiting in a delayed queue.
-- ARGV: 1 result key, 2 task-data key, 3 abort key, 4 retry key, 5 running set,
--       6 results index, 7 task id, 8 aborted result payload, 9 marker ttl ms,
--       10 result ttl ms (0 = do not store, < 0 = keep forever), 11 result
--       channel, 12 abort channel, 13 now ms, 14.. delayed keys (one per priority).
-- Returns 1 if the abort was accepted, 0 if the task is already done or unknown.
local result_key    = ARGV[1]
local task_key      = ARGV[2]
local abort_key     = ARGV[3]
local retry_key     = ARGV[4]
local running_set   = ARGV[5]
local results_index = ARGV[6]
local task_id       = ARGV[7]
local payload       = ARGV[8]
local marker_ttl    = ARGV[9]
local result_ttl    = tonumber(ARGV[10])
local result_chan   = ARGV[11]
local abort_chan    = ARGV[12]
local now           = tonumber(ARGV[13])

if redis.call('EXISTS', result_key) == 1 then return 0 end
if redis.call('EXISTS', task_key) == 0 then return 0 end

redis.call('SET', abort_key, '1', 'PX', marker_ttl)
redis.call('PUBLISH', abort_chan, task_id)

-- Running: the worker holding it gets the publish and finalizes the task itself.
if redis.call('SISMEMBER', running_set, task_id) == 1 then return 1 end

-- Not running. If it is waiting in a delayed queue we own it and can finalize
-- now; otherwise it sits in a live stream and a worker honors the marker at
-- pickup (a stream entry can only be dropped by the consumer that reads it).
local removed = 0
for i = 14, #ARGV do
  removed = removed + redis.call('ZREM', ARGV[i], task_id)
end
if removed == 0 then return 1 end

redis.call('DEL', task_key, abort_key, retry_key)
if result_ttl > 0 then
  redis.call('SET', result_key, payload, 'PX', result_ttl)
  redis.call('ZADD', results_index, now + result_ttl, task_id)
  redis.call('PUBLISH', result_chan, task_id)
elseif result_ttl < 0 then
  redis.call('SET', result_key, payload)
  redis.call('ZADD', results_index, now + 100000000000, task_id)  -- FAR_FUTURE_MS
  redis.call('PUBLISH', result_chan, task_id)
end
return 1
