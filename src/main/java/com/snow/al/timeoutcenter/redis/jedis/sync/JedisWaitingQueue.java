package com.snow.al.timeoutcenter.redis.jedis.sync;

import com.snow.al.timeoutcenter.*;
import com.snow.al.timeoutcenter.redis.jedis.async.AsyncHandleFactory;
import com.snow.al.timeoutcenter.redis.jedis.async.AsyncHandleListener;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.snow.al.timeoutcenter.TimeoutTask.calKey;

@Slf4j
public class JedisWaitingQueue extends WaitingQueue {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;
    private final String zsetKey;
    private final HandleFactory handleFactory;
    private final AsyncHandleFactory asyncHandleFactory;

    public JedisWaitingQueue(HandleQueue handleQueue, JedisPool pool, String bizTag, int slotNumber, HandleFactory handleFactory) {
        super(handleQueue);
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotNumber = slotNumber;
        this.zsetKey = calKey(getQueueType(), bizTag, slotNumber);
        this.handleFactory = handleFactory;
        ExecutorService performTaskThreadPool = new ThreadPoolExecutor(5, 20, 60, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1024),
                new ThreadFactory() {
                    private final AtomicInteger threadNum = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread a = new Thread(r);
                        a.setName("waitingQueue-performTask-thread-" + threadNum.getAndIncrement());
                        a.setDaemon(false);
                        return a;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.asyncHandleFactory = new AsyncHandleFactory(this.handleFactory, performTaskThreadPool);

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
        Tuple tuple = JedisClient.moveTimeoutMemberFrom2To(pool, zsetKey, calKey(HandleQueue.QUEUE_TYPE, bizTag, slotNumber));
        if (tuple != null) {
            TimeoutTask timeoutTask = new TimeoutTask(tuple);
            asyncHandleFactory.asyncHandle(timeoutTask, new AsyncHandleListener() {
                @Override
                public void onSuccess(boolean result, TimeoutTask timeoutTask) {
                    if (result) {
                        handleQueue.del(timeoutTask);
                    } else {
                        executeAfterPerformTaskFail(timeoutTask);
                    }
                }

                @Override
                public void onFailure(Exception e, TimeoutTask timeoutTask) {
                    log.error("handle task failed, task:{}, error:{}", timeoutTask, e.getMessage(), e);
                    executeAfterPerformTaskFail(timeoutTask);
                }
            });
        }
    }

    private void executeAfterPerformTaskFail(TimeoutTask timeoutTask) {
        timeoutTask.increaseRetryNumber();
        if (timeoutTask.getRetryNumber() >= 16) {
            JedisClient.moveMemberFrom2To(pool,
                    calKey(HandleQueue.QUEUE_TYPE, bizTag, slotNumber),
                    calKey(DeadLetterQueue.QUEUE_TYPE, bizTag, slotNumber),
                    timeoutTask.calValue(),
                    String.valueOf(timeoutTask.calScore()));
            return;
        }
        JedisClient.moveMemberFrom2To(pool,
                calKey(HandleQueue.QUEUE_TYPE, bizTag, slotNumber),
                calKey(WaitingQueue.QUEUE_TYPE, bizTag, slotNumber),
                timeoutTask.calValue(),
                String.valueOf(timeoutTask.calScore()));
    }
}
