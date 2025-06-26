-- [[
it is used to move the member from a zset to another one
KEYS[1], fromZSet key
KEYS[2], toZSet key
KEYS[3], score
KEYS[4], member value
]]--
local del_cnt = redis.pcall('ZREM', KEYS[1], ARGV[2])
local add_cnt = 0
if del_cnt > 0 then
    add_cnt = redis.pcall('zadd', KEYS[2], ARGV[1], ARGV[2])
end
return add_cnt

-- eval "local del_cnt = redis.pcall('ZREM', KEYS[1], KEYS[4]) local add_cnt = 0 if del_cnt > 0 then     add_cnt = redis.pcall('zadd', KEYS[2], KEYS[3], KEYS[4]) end return add_cnt" 2 key1 key2 5 v5
