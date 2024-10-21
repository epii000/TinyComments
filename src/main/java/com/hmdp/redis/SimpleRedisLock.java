package com.hmdp.redis;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String name;
    private StringRedisTemplate redisTemplate;
    public static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    public SimpleRedisLock(StringRedisTemplate redisTemplate, String name) {
        this.redisTemplate = redisTemplate;
        this.name = name;
    }


    @Override
    public boolean tryLock(Long timeoutSec) {
        // 获取线程标志
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean success = redisTemplate.opsForValue()
                .setIfAbsent(RedisConstants.KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // 获取线程标志
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁标志
        String id = redisTemplate.opsForValue().get(RedisConstants.KEY_PREFIX + name);
        if(id.equals(threadId)) {
            // 释放锁
            redisTemplate.delete(RedisConstants.KEY_PREFIX + name);
        }
    }
}
