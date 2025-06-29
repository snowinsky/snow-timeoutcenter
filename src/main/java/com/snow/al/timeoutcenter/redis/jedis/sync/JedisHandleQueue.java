package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.DeadLetterQueue;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.HandleQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Optional;

import static com.snow.al.timeoutcenter.TimeoutTask.calKey;

public class JedisHandleQueue extends HandleQueue {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;


    public JedisHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue, JedisPool pool, String bizTag, int slotNumber) {
        super(handleFactory, deadLetterQueue);
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotNumber = slotNumber;
    }

    @Override
    public String getBizTag() {
        return bizTag;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            long ret = jedis.zadd(TimeoutTask.calKey(QUEUE_TYPE, bizTag, slotNumber), timeoutTask.calScore(), timeoutTask.calValue());
            return ret > 0;
        }
    }

    @Override
    public TimeoutTask peek() {
        try (Jedis jedis = pool.getResource()) {
            List<Tuple> t = jedis.zrangeWithScores(calKey(QUEUE_TYPE, bizTag, slotNumber), 0, 0);
            if (t == null || t.isEmpty()) {
                return null;
            }
            return t.stream().findFirst().map(TimeoutTask::new).orElse(null);
        }
    }

    @Override
    public TimeoutTask poll() {
        try (Jedis jedis = pool.getResource()) {
            Tuple t = jedis.zpopmin(calKey(QUEUE_TYPE, bizTag, slotNumber));
            return Optional.ofNullable(t).map(TimeoutTask::new).orElse(null);
        }
    }
}
