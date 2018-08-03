package redisqueue

import "github.com/gomodule/redigo/redis"

var popJobsScript = redis.NewScript(3, `
local key_queue = KEYS[1]
local value_queue = key_queue .. ':values'
local timestamp = KEYS[2]
local limit = KEYS[3]
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
local _, job = cmsgpack.unpack_one(ARGV[1])
if redis.call('zadd', key_queue, job.when, job.id) ~= 1 then
	return 0
end
return redis.call('hset', value_queue, job.id, job.body)`)

var removeScript = redis.NewScript(2, `
local key_queue = KEYS[1]
local value_queue = key_queue .. ':values'
local key = KEYS[2]
if redis.call('zrem', key_queue, key) ~= 1 then
	return 0
end
return redis.call('hdel', value_queue, key)`)
