--[[ 判断优惠券库存是否够  库存不够返回1  然后判断用户是否下过单了 下过单返回2  没下过单库存也够 返回0 ]]
local voucherId = ARGV[1]
local userId = ARGV[2]
local orderId = ARGV[3]


-- 2.数据key
-- 2.1.库存key  .. 是 Lua 中的字符串拼接操作符
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId

-- 第一步 判断库存够不够
if(tonumber(redis.call('get',stockKey)) <= 0) then
    return 1
end

-- 第二步 判断用户是否下过单
if(redis.call('sismember',orderKey,userId) == 1) then
    return 2
end

-- 没下过单 库存也够 则将用户的id放入下过单的集合中 库存减一
redis.call('incrby',stockKey,-1)

redis.call('sadd',orderKey,userId)
-- 把信息存入redis  现在利用redis地stream实现消息队列 将要下单存入数据库地订单信息放入redis而不是阻塞队列 防止信息的丢失 和 减少JVM内存的使用
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0

