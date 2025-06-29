package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.SnowTimeoutCenter;
import com.snow.al.timeoutcenter.TimeoutTask;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LuaRedisJedisTimeoutCenterBootstrap implements SnowTimeoutCenter {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotCount;

    private final LuaRedisJedisTimeoutCenter[] timeoutCenters;
    private final DeadLetterHandleFactory deadLetterHandleFactory;
    private final HandleFactory handleFactory;

    public LuaRedisJedisTimeoutCenterBootstrap(JedisPool pool, String bizTag, int slotCount, DeadLetterHandleFactory deadLetterHandleFactory, HandleFactory handleFactory) {
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotCount = slotCount;
        this.timeoutCenters = new LuaRedisJedisTimeoutCenter[slotCount];
        this.deadLetterHandleFactory = deadLetterHandleFactory;
        this.handleFactory = handleFactory;
    }

    @Override
    public void start() {
        for (int i = 0; i < slotCount; i++) {
            LuaRedisJedisTimeoutCenter timeoutCenter = new CentainLuaRedisJedisTimeoutCenter(pool, bizTag, i, deadLetterHandleFactory, handleFactory);
            timeoutCenter.start();
            timeoutCenters[i] = timeoutCenter;
        }
    }

    private class CentainLuaRedisJedisTimeoutCenter extends LuaRedisJedisTimeoutCenter {
        private final DeadLetterHandleFactory deadLetterHandleFactory;
        private final HandleFactory handleFactory;

        public CentainLuaRedisJedisTimeoutCenter(JedisPool pool, String bizTag, int slotNumber, DeadLetterHandleFactory deadLetterHandleFactory, HandleFactory handleFactory) {
            super(pool, bizTag, slotNumber);
            this.deadLetterHandleFactory = deadLetterHandleFactory;
            this.handleFactory = handleFactory;
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

    @Override
    public void shutdown() {
        for (LuaRedisJedisTimeoutCenter timeoutCenter : timeoutCenters) {
            timeoutCenter.shutdown();
        }
    }

    @Override
    public boolean publish(TimeoutTask timeoutTask) {
        String ss = timeoutTask.getTaskFrom() + "##" + timeoutTask.getTaskFromId();
        int index = Math.abs(ss.hashCode() % slotCount);
        return timeoutCenters[index].publish(timeoutTask);
    }

    public static void main(String[] args) {
        JedisPool pool = JedisClient.getJedisPool();
        int slotCount = 2;
        String bizTag = "AABSCCC";

        ExecutorService es = Executors.newFixedThreadPool(3);

            LuaRedisJedisTimeoutCenterBootstrap timeoutCenter1 = new LuaRedisJedisTimeoutCenterBootstrap(pool, bizTag, slotCount,
                    timeoutTask -> System.out.println("deadLetterHandleFactory1:" + timeoutTask),
                    timeoutTask -> {
                System.out.println("handleFactory1:" + timeoutTask);
                return true;
            });
            timeoutCenter1.start();
            LuaRedisJedisTimeoutCenterBootstrap timeoutCenter2 = new LuaRedisJedisTimeoutCenterBootstrap(pool, bizTag, slotCount,
                    timeoutTask -> System.out.println("deadLetterHandleFactory2:" + timeoutTask),
                    timeoutTask -> {
                        System.out.println("handleFactory2:" + timeoutTask);
                        return true;
                    });
            timeoutCenter2.start();
            for (int j = 0; j < 100; j++) {
                TimeoutTask tt = new TimeoutTask();
                tt.setTaskFrom("AABSCCC");
                tt.setTaskFromId("sdfdsfasdf" + System.nanoTime());
                tt.setTaskTimeout(System.currentTimeMillis() + 3000 + j);
                es.execute(()->timeoutCenter1.publish(tt));
            }

    }
}
