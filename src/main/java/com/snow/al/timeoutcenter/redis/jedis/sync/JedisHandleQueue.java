package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.HandleQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import com.snow.al.timeoutcenter.WaitingQueue;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeUnit;

import static com.snow.al.timeoutcenter.TimeoutTask.calKey;

@Slf4j
public class JedisHandleQueue extends HandleQueue {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;
    private final String zsetKey;


    public JedisHandleQueue(JedisPool pool, String bizTag, int slotNumber) {
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotNumber = slotNumber;
        this.zsetKey = calKey(QUEUE_TYPE, bizTag, slotNumber);
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
    protected void startCore() {
        JedisClient.moveTimeoutMembersFrom2To(pool,
                calKey(HandleQueue.QUEUE_TYPE, bizTag, slotNumber),
                calKey(WaitingQueue.QUEUE_TYPE, bizTag, slotNumber),
                30, TimeUnit.SECONDS);
    }
}
