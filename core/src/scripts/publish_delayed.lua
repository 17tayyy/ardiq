-- Promote due delayed tasks to the live stream atomically (one priority).
-- ARGV: 1 delayed key, 2 stream key, 3 now ms. Returns the number promoted.
local delayed = ARGV[1]
local stream  = ARGV[2]
local now     = tonumber(ARGV[3])

local due = redis.call('ZRANGE', delayed, 0, now, 'BYSCORE', 'WITHSCORES')
if #due == 0 then return 0 end
redis.call('ZREMRANGEBYSCORE', delayed, 0, now)
for i = 1, #due, 2 do
  redis.call('XADD', stream, '*', 'task_id', due[i], 'enqueue_time', due[i + 1])
end
return #due / 2
