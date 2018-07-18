package redisqueue

import "github.com/gomodule/redigo/redis"

var popJobsScript = redis.NewScript(3, `
		local key_queue = KEYS[1]
		local value_queue = key_queue .. ':values'
		local timestamp = KEYS[2]
		local limit = KEYS[3]
		local keys = redis.call('zrangebyscore', key_queue, '-inf', timestamp, 'LIMIT', 0, limit)
		local values = {}
		if table.getn(keys) > 0 then
			values = redis.call('hmget', value_queue, unpack(keys))
			redis.call('zrem', key_queue, unpack(keys))
			redis.call('hdel', value_queue, unpack(keys))
		end
		return values
  `)
