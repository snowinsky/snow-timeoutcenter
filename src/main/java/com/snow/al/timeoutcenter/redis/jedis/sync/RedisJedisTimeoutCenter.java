package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@Slf4j
@RequiredArgsConstructor
public class RedisJedisTimeoutCenter extends AbstractSnowTimeoutCenter {

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
        return new JedisHandleQueue(pool, bizTag, slotNumber);
    }

    @Override
    protected WaitingQueue initWaitingQueue(HandleQueue handleQueue, HandleFactory handleFactory) {
        return new JedisWaitingQueue(handleQueue, pool, bizTag, slotNumber, handleFactory);
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
