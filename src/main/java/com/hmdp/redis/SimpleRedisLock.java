package com.hmdp.redis;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock {

    private String name;
    public static final String ID_PREFIX = UUID.randomUUID().toString(true) + "-";
    // 锁的唯一标识，用UUID区分不同的服务（也就是不同的jvm），再用不同的线程id区分不同的线程，二者结合确保不同线程标识不同，相同线程标识相同
    private StringRedisTemplate stringRedisTemplate;

    // 定义锁的脚本（Lua脚本）
    public static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }


    @Override
    public boolean tryLock(Long timeoutSec) {
        // 获取线程标志
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue()
                .setIfAbsent(RedisConstants.KEY_PREFIX + name, threadId, timeoutSec, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unlock() {
        // 调用lua脚本
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(RedisConstants.KEY_PREFIX + name),
                ID_PREFIX + Thread.currentThread().getId());
    }

    /*@Override
    public void unlock() {
        // 获取线程标志
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        // 获取锁标志
        String id = redisTemplate.opsForValue().get(RedisConstants.KEY_PREFIX + name);
        if(id.equals(threadId)) {
            // 释放锁
            redisTemplate.delete(RedisConstants.KEY_PREFIX + name);
        }
    }*/
}
