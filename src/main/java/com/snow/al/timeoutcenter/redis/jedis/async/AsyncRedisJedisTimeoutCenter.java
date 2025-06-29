package com.snow.al.timeoutcenter.redis.jedis.async;

import com.snow.al.timeoutcenter.*;
import com.snow.al.timeoutcenter.redis.jedis.sync.JedisDeadLetterQueue;
import com.snow.al.timeoutcenter.redis.jedis.sync.LuaJedisWaitingQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@Slf4j
@RequiredArgsConstructor
public class AsyncRedisJedisTimeoutCenter extends AbstractTimeoutCenterFacade {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;
    private final DeadLetterHandleFactory deadLetterHandleFactory;
    private final HandleFactory handleFactory;

    @Override
    protected DeadLetterQueue initDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory) {
        return new JedisDeadLetterQueue(deadLetterHandleFactory, pool, bizTag, slotNumber) ;
    }

    @Override
    protected HandleQueue initHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue) {
        return new AsyncJedisHandleQueue(handleFactory, deadLetterQueue, pool, bizTag, slotNumber);
    }

    @Override
    protected WaitingQueue initWaitingQueue(HandleQueue handleQueue) {
        return new LuaJedisWaitingQueue(handleQueue, pool, bizTag, slotNumber);
    }

    @Override
    protected DeadLetterHandleFactory initSingletonDeadLetterHandleFactory() {
        return deadLetterHandleFactory;
    }

    @Override
    protected HandleFactory initSingletonHandleFactory() {
        return handleFactory;
    }

}
