package com.snow.al.timeoutcenter.redis.jedis;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.DeadLetterQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Optional;

import static com.snow.al.timeoutcenter.TimeoutTask.calKey;

public class JedisDeadLetterQueue extends DeadLetterQueue {

    private final JedisPool pool;
    private final int slotNumber;

    public JedisDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory, JedisPool pool, int slotNumber) {
        super(deadLetterHandleFactory);
        this.pool = pool;
        this.slotNumber = slotNumber;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            long ret = jedis.zadd(TimeoutTask.calKey(QUEUE_TYPE, slotNumber), timeoutTask.calScore(), timeoutTask.calValue());
            return ret > 0;
        }
    }

    @Override
    public TimeoutTask peek() {
        try (Jedis jedis = pool.getResource()) {
            List<Tuple> t = jedis.zrangeWithScores(calKey(QUEUE_TYPE, slotNumber), 0, 0);
            if (t == null || t.isEmpty()) {
                return null;
            }
            return t.stream().findFirst().map(TimeoutTask::new).orElse(null);
        }
    }

    @Override
    public TimeoutTask poll() {
        try (Jedis jedis = pool.getResource()) {
            Tuple t = jedis.zpopmin(calKey(QUEUE_TYPE, slotNumber));
            return Optional.ofNullable(t).map(TimeoutTask::new).orElse(null);
        }
    }
}
