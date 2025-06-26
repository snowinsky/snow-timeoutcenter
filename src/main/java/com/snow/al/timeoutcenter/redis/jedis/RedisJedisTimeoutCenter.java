package com.snow.al.timeoutcenter.redis.jedis;

import com.snow.al.timeoutcenter.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public class RedisJedisTimeoutCenter extends AbstractTimeoutCenterFacade {

    private final JedisPool pool;
    private final int slotNumber;

    @Override
    protected DeadLetterQueue initDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory) {
        return new JedisDeadLetterQueue(deadLetterHandleFactory, pool, slotNumber) ;
    }

    @Override
    protected HandleQueue initHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue) {
        return new JedisHandleQueue(handleFactory, deadLetterQueue, pool, slotNumber);
    }

    @Override
    protected WaitingQueue initWaitingQueue(HandleQueue handleQueue) {
        return new JedisWaitingQueue(handleQueue, pool, slotNumber);
    }

    @Override
    protected DeadLetterHandleFactory initSingletonDeadLetterHandleFactory() {
        return new DeadLetterHandleFactory() {
            @Override
            public void handleTimeoutTask(TimeoutTask timeoutTask) {
                log.info("jedis process the task:{}", timeoutTask);
            }
        };
    }

    @Override
    protected HandleFactory initSingletonHandleFactory() {
        return new HandleFactory() {
            @Override
            public boolean performTask(TimeoutTask timeoutTask) throws TimeoutException {
                log.info("jedis receive the task:{}", timeoutTask);
                return true;
            }
        };
    }

}
