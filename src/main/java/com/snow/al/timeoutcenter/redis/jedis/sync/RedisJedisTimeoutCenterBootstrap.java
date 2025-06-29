package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.SnowTimeoutCenter;
import com.snow.al.timeoutcenter.TimeoutTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@RequiredArgsConstructor
@Slf4j
public class RedisJedisTimeoutCenterBootstrap implements SnowTimeoutCenter {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotCount;

    private final RedisJedisTimeoutCenter[] timeoutCenters;

    public RedisJedisTimeoutCenterBootstrap(JedisPool pool, String bizTag, int slotCount) {
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotCount = slotCount;
        this.timeoutCenters = new RedisJedisTimeoutCenter[slotCount];
    }

    @Override
    public void start() {
        for (int i = 0; i < slotCount; i++) {
            RedisJedisTimeoutCenter timeoutCenter = new RedisJedisTimeoutCenter(pool, bizTag, i);
            timeoutCenter.start();
            timeoutCenters[i] = timeoutCenter;
        }
    }

    @Override
    public void shutdown() {
        for (RedisJedisTimeoutCenter timeoutCenter : timeoutCenters) {
            timeoutCenter.shutdown();
        }
    }

    @Override
    public boolean publish(TimeoutTask timeoutTask) {
        String ss = timeoutTask.getTaskFrom() + "##" + timeoutTask.getTaskFromId();
        int index = ss.hashCode() % slotCount;
        return timeoutCenters[index].publish(timeoutTask);
    }
}
