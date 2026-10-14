---@diagnostic disable: undefined-global
-- Drop one chunk of the work waiting in one lane: tasks in its delayed set, and
-- stream entries up to `last` that the group has not delivered yet. Delivered
-- entries (running or prefetched) stay. Call it until both counts are 0.
-- ARGV: 1 stream key, 2 delayed key, 3 group, 4 task key prefix ("ardiq:q:task:"),
--       5 last stream id to drop ("" = none), 6 stream chunk, 7 delayed chunk.
-- Returns {waiting dropped, scheduled dropped}.
local stream  = ARGV[1]
local delayed = ARGV[2]
local group   = ARGV[3]
local prefix  = ARGV[4]
local last    = ARGV[5]
local chunk   = tonumber(ARGV[6])
local take    = tonumber(ARGV[7])

local function forget(task_id)
  redis.call('DEL', prefix .. 'data:' .. task_id, prefix .. 'retry:' .. task_id,
    prefix .. 'abort:' .. task_id)
end

local scheduled = {}
if take > 0 then
  scheduled = redis.call('ZRANGE', delayed, 0, take - 1)
  for _, task_id in ipairs(scheduled) do
    forget(task_id)
  end
  if #scheduled > 0 then
    redis.call('ZREM', delayed, unpack(scheduled))
  end
end

local waiting = {}
if last ~= '' and redis.call('EXISTS', stream) == 1 then
  -- Everything after the group's last-delivered id is still waiting.
  local delivered = '0-0'
  for _, group_info in ipairs(redis.call('XINFO', 'GROUPS', stream)) do
    local fields = {}
    for i = 1, #group_info, 2 do
      fields[group_info[i]] = group_info[i + 1]
    end
    if fields['name'] == group then
      delivered = fields['last-delivered-id']
    end
  end
  for _, entry in ipairs(redis.call('XRANGE', stream, '(' .. delivered, last, 'COUNT', chunk)) do
    local fields = entry[2]
    for i = 1, #fields, 2 do
      if fields[i] == 'task_id' then
        forget(fields[i + 1])
      end
    end
    waiting[#waiting + 1] = entry[1]
  end
  if #waiting > 0 then
    redis.call('XDEL', stream, unpack(waiting))
  end
end

return {#waiting, #scheduled}
