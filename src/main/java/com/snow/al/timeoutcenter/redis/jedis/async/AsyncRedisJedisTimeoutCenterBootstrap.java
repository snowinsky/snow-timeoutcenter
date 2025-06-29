package com.snow.al.timeoutcenter.redis.jedis.async;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.SnowTimeoutCenter;
import com.snow.al.timeoutcenter.TimeoutTask;
import com.snow.al.timeoutcenter.redis.jedis.sync.JedisClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

@RequiredArgsConstructor
@Slf4j
public class AsyncRedisJedisTimeoutCenterBootstrap implements SnowTimeoutCenter {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotCount;
    private final DeadLetterHandleFactory deadLetterHandleFactory;
    private final HandleFactory handleFactory;

    private final AsyncRedisJedisTimeoutCenter[] timeoutCenters;

    public AsyncRedisJedisTimeoutCenterBootstrap(JedisPool pool, String bizTag, int slotCount, DeadLetterHandleFactory deadLetterHandleFactory, HandleFactory handleFactory) {
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotCount = slotCount;
        this.timeoutCenters = new AsyncRedisJedisTimeoutCenter[slotCount];
        this.deadLetterHandleFactory = deadLetterHandleFactory;
        this.handleFactory = handleFactory;
    }

    @Override
    public void start() {
        for (int i = 0; i < slotCount; i++) {
            AsyncRedisJedisTimeoutCenter timeoutCenter = new AsyncRedisJedisTimeoutCenter(pool, bizTag, i, deadLetterHandleFactory, handleFactory);
            timeoutCenter.start();
            timeoutCenters[i] = timeoutCenter;
        }
    }

    @Override
    public void shutdown() {
        for (AsyncRedisJedisTimeoutCenter timeoutCenter : timeoutCenters) {
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
        var aa = new AsyncRedisJedisTimeoutCenterBootstrap(JedisClient.getJedisPool(), "vendor-batch", 3,
                a -> {
            log.info("aa=dead letter handle" + a);
                }, a -> {
            log.info("aa=handle" + a);
            return false;
        });
        aa.start();
        /*var aa1 = new AsyncRedisJedisTimeoutCenterBootstrap(JedisClient.getJedisPool(), "vendor-batch", 3,
                a -> {
            log.info("aa1=dead letter handle" + a);
                }, a -> {
            log.info("aa1=handle" + a);
            return false;
        });
        aa1.start();*/
        for (int i = 0; i < 10; i++) {
            TimeoutTask tt = new TimeoutTask();
            tt.setTaskFrom("CCB");
            tt.setTaskFromId(System.nanoTime() + "");
            tt.setTaskTimeout(System.currentTimeMillis() + i*1000);
            aa.publish(tt);
        }
    }
}
