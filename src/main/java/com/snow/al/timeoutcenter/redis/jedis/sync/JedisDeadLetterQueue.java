package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.DeadLetterQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.Optional;

public class JedisDeadLetterQueue extends DeadLetterQueue {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;
    private final String zsetKey;

    public JedisDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory, JedisPool pool, String bizTag, int slotNumber) {
        super(deadLetterHandleFactory);
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotNumber = slotNumber;
        this.zsetKey = TimeoutTask.calKey(QUEUE_TYPE, bizTag, this.slotNumber);
    }

    @Override
    public String getBizTag() {
        return bizTag;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            long ret = jedis.zadd(zsetKey, timeoutTask.calScore(), timeoutTask.calValue());
            return ret > 0;
        }
    }

    @Override
    public boolean del(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            long ret = jedis.zrem(zsetKey, timeoutTask.calValue());
            return ret > 0;
        }
    }


    @Override
    protected TimeoutTask poll() {
        try (Jedis jedis = pool.getResource()) {
            Tuple tuple = jedis.zpopmin(zsetKey);
            return Optional.ofNullable(tuple).map(TimeoutTask::new).orElse(null);
        }
    }
}
