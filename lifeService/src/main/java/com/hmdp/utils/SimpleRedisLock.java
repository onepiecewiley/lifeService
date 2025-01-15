package com.hmdp.utils;

import cn.hutool.core.io.resource.ClassPathResource;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author onepiecewiley
 * @version 1.0
 * @date 2024/11/18 12:40
 */
public class SimpleRedisLock implements ILock {
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PREFIX = UUID.randomUUID() + "-";

    private static final DefaultRedisScript<Long> UNLOCK_LUA;

    static {
            UNLOCK_LUA = new DefaultRedisScript<>();
            UNLOCK_LUA.setLocation((Resource) new ClassPathResource("unlock.lua"));
            UNLOCK_LUA.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程id
        String threadId = ID_PREFIX + Thread.currentThread().getId() + "";
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }

    @Override
    public void unlock() {
        stringRedisTemplate.execute(UNLOCK_LUA,Collections.singletonList(ID_PREFIX + Thread.currentThread().getId()) ,ID_PREFIX + Thread.currentThread().getId());
    }

/*
    有原子性的问题 不行 存在线程安全问题
    @Override
    public void unlock() {
        // 获取线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId() + "";

        //获取锁的标识
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        // 这里必须要做判断 是为了防止某个业务时间太长导致锁释放了但是业务没有完成 后期业务完成后释放另一个业务的锁导致的线程并发安全问题
        if(id.equals(threadId)){
            //释放锁
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }
    }*/
}
