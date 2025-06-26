package com.snow.al.timeoutcenter.redis.jedis;

import com.snow.al.timeoutcenter.SnowTimeoutCenter;
import com.snow.al.timeoutcenter.TimeoutTask;
import junit.framework.TestCase;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.Executors;

public class RedisJedisTimeoutCenterTest extends TestCase {

    public void testInitDeadLetterQueue() {

        JedisPool pool = JedisClient.getJedisPool();
        int slotNumber = 1;

        SnowTimeoutCenter snowTimeoutCenter = new RedisJedisTimeoutCenter(pool, slotNumber);
        snowTimeoutCenter.start();

        var threadPool = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 50; i++) {
            TimeoutTask tt = new TimeoutTask();
            tt.setTaskFrom("AABSCCC");
            tt.setTaskFromId("sdfdsfasdf" + System.nanoTime());
            tt.setTaskTimeout(System.currentTimeMillis() + i);

            threadPool.execute(()->snowTimeoutCenter.publish(tt));
        }

        threadPool.shutdown();
    }
}