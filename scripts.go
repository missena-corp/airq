package redisqueue

import "github.com/gomodule/redigo/redis"

var popJobsScript = redis.NewScript(1, `
local key_queue = KEYS[1]
local value_queue = key_queue .. ':values'
local timestamp = ARGV[1]
local limit = ARGV[2]
local keys = redis.call('zrangebyscore', key_queue, '-inf', timestamp, 'LIMIT', 0, limit)
if table.getn(keys) == 0 then
	return {}
end
local values = redis.call('hmget', value_queue, unpack(keys))
redis.call('zrem', key_queue, unpack(keys))
redis.call('hdel', value_queue, unpack(keys))
return values`)

var pushScript = redis.NewScript(1, `
local key_queue = KEYS[1]
local value_queue = key_queue .. ':values'
local ids = {}
for i=1, #ARGV do
	local _, job = cmsgpack.unpack_one(ARGV[i])
	if redis.call('zadd', key_queue, job.when, job.id) ~= 1 then
		return ids
	end
	if redis.call('hset', value_queue, job.id, job.body) ~= 1 then
		return ids
	end
	table.insert(ids, job.id)
end
return ids`)

var removeScript = redis.NewScript(1, `
local key_queue = KEYS[1]
local value_queue = key_queue .. ':values'
redis.call('zrem', key_queue, unpack(ARGV))
return redis.call('hdel', value_queue, unpack(ARGV))`)
