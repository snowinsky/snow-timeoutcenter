package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.TimeoutTask;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.concurrent.TimeoutException;

@Slf4j
public class RedisJedisTimeoutCenterBootstrapTest extends TestCase {

    public void testPublish() throws InterruptedException {
        JedisPool pool = JedisClient.getJedisPool();

        RedisJedisTimeoutCenterBootstrap bootstrap = new RedisJedisTimeoutCenterBootstrap(pool, "test", 2,
                new DeadLetterHandleFactory() {

                    @Override
                    public void handleTimeoutTask(TimeoutTask timeoutTask) {
                        log.info("test0=dead letter handle {}", timeoutTask);
                    }
                },
                new HandleFactory() {
                    @Override
                    public boolean performTask(TimeoutTask timeoutTask) throws TimeoutException {
                        log.info("test0=handle {}", timeoutTask);
                        return false;
                    }

                });
        bootstrap.start();
        RedisJedisTimeoutCenterBootstrap bootstrap1 = new RedisJedisTimeoutCenterBootstrap(pool, "test", 2,
                new DeadLetterHandleFactory() {

                    @Override
                    public void handleTimeoutTask(TimeoutTask timeoutTask) {
                        log.info("test1=dead letter handle {}", timeoutTask);
                    }
                },
                new HandleFactory() {
                    @Override
                    public boolean performTask(TimeoutTask timeoutTask) throws TimeoutException {
                        log.info("test1=handle {}", timeoutTask);
                        return false;
                    }

                });
        bootstrap1.start();
        for (int i = 0; i < 1; i++) {
            TimeoutTask tt = new TimeoutTask();
            tt.setTaskFrom("ABC");
            tt.setTaskFromId("" + System.nanoTime());
            tt.setTaskTimeout(System.currentTimeMillis() + i * 1000);
            bootstrap.publish(tt);
        }

        Thread.sleep(30000);

    }
}