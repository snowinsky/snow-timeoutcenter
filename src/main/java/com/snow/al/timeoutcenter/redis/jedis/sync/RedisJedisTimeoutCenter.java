package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@Slf4j
@RequiredArgsConstructor
public class RedisJedisTimeoutCenter extends AbstractTimeoutCenterFacade {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;

    @Override
    protected DeadLetterQueue initDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory) {
        return new JedisDeadLetterQueue(deadLetterHandleFactory, pool, bizTag, slotNumber) ;
    }

    @Override
    protected HandleQueue initHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue) {
        return new JedisHandleQueue(handleFactory, deadLetterQueue, pool, bizTag, slotNumber);
    }

    @Override
    protected WaitingQueue initWaitingQueue(HandleQueue handleQueue) {
        return new JedisWaitingQueue(handleQueue, pool, bizTag, slotNumber);
    }

    @Override
    protected DeadLetterHandleFactory initSingletonDeadLetterHandleFactory() {
        return timeoutTask -> log.info("jedis process the task:{}", timeoutTask);
    }

    @Override
    protected HandleFactory initSingletonHandleFactory() {
        return timeoutTask -> {
            log.info("jedis receive the task:{}", timeoutTask);
            return true;
        };
    }

}
