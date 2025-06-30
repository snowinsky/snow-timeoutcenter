package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class RedisJedisTimeoutCenterBootstrap implements SnowTimeoutCenter {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotCount;
    private final DeadLetterHandleFactory deadLetterHandleFactory;
    private final HandleFactory handleFactory;

    private final RedisJedisTimeoutCenter[] timeoutCenters;

    private volatile boolean isStarted = false;

    public RedisJedisTimeoutCenterBootstrap(JedisPool pool, String bizTag, int slotCount, DeadLetterHandleFactory deadLetterHandleFactory, HandleFactory handleFactory) {
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotCount = slotCount;
        this.timeoutCenters = new RedisJedisTimeoutCenter[slotCount];
        this.deadLetterHandleFactory = deadLetterHandleFactory;
        this.handleFactory = handleFactory;
    }

    @Override
    public void start() {
        for (int i = 0; i < slotCount; i++) {
            RedisJedisTimeoutCenter timeoutCenter = new RedisJedisTimeoutCenter(pool, bizTag, i, deadLetterHandleFactory, handleFactory);
            timeoutCenter.start();
            timeoutCenters[i] = timeoutCenter;
        }
        isStarted = true;
    }

    @Override
    public void shutdown() {
        for (RedisJedisTimeoutCenter timeoutCenter : timeoutCenters) {
            Optional.ofNullable(timeoutCenter).ifPresent(AbstractSnowTimeoutCenter::shutdown);
        }
        isStarted = false;
    }

    @Override
    public boolean publish(TimeoutTask timeoutTask) {
        if (!isStarted) {
            log.error("timeout center is not started, bizTag={}, slotCount={}", bizTag, slotCount);
            throw new IllegalStateException("timeout center is not started");
        }
        String ss = timeoutTask.getTaskFrom() + "##" + timeoutTask.getTaskFromId();
        int index = Math.abs(ss.hashCode() % slotCount);
        SnowTimeoutCenter cc = timeoutCenters[index];
        return cc.publish(timeoutTask);
    }
}
