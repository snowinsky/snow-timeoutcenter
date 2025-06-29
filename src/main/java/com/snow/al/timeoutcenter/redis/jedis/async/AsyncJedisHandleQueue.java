package com.snow.al.timeoutcenter.redis.jedis.async;

import com.snow.al.timeoutcenter.DeadLetterQueue;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.HandleQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import com.snow.al.timeoutcenter.redis.jedis.sync.JedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.snow.al.timeoutcenter.TimeoutTask.calKey;

public class AsyncJedisHandleQueue extends HandleQueue {

    private final JedisPool pool;
    private final String bizTag;
    private final int slotNumber;
    private final AsyncHandleFactory asyncHandleFactory;


    public AsyncJedisHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue, JedisPool pool, String bizTag, int slotNumber) {
        super(handleFactory, deadLetterQueue);
        this.pool = pool;
        this.bizTag = bizTag;
        this.slotNumber = slotNumber;
        ExecutorService performTaskThreadPool = new ThreadPoolExecutor(5, 20, 60, java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<>(1024),
                new ThreadFactory() {
                    private final AtomicInteger threadNum = new AtomicInteger(1);

                    @Override
                    public Thread newThread(Runnable r) {
                        Thread a = new Thread(r);
                        a.setName("handleQueue-performTask-thread-" + threadNum.getAndIncrement());
                        a.setDaemon(false);
                        return a;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.asyncHandleFactory = new AsyncHandleFactory(handleFactory, performTaskThreadPool);
    }

    @Override
    public String getBizTag() {
        return bizTag;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            long ret = jedis.zadd(TimeoutTask.calKey(QUEUE_TYPE, bizTag, slotNumber), timeoutTask.calScore(), timeoutTask.calValue());
            return ret > 0;
        }
    }

    @Override
    public TimeoutTask peek() {
        try (Jedis jedis = pool.getResource()) {
            List<Tuple> t = jedis.zrangeWithScores(calKey(QUEUE_TYPE, bizTag, slotNumber), 0, 0);
            if (t == null || t.isEmpty()) {
                return null;
            }
            return t.stream().findFirst().map(TimeoutTask::new).orElse(null);
        }
    }

    @Override
    public TimeoutTask poll() {
        try (Jedis jedis = pool.getResource()) {
            Tuple t = jedis.zpopmin(calKey(QUEUE_TYPE, bizTag, slotNumber));
            return Optional.ofNullable(t).map(TimeoutTask::new).orElse(null);
        }
    }

    @Override
    public void start() {
        isStart = true;
        while (true) {
            if (!isStart) {
                return;
            }
            TimeoutTask timeoutTask = poll();
            if (timeoutTask == null) {
                continue;
            }
            asyncHandleFactory.asyncHandle(timeoutTask, new AsyncHandleListener() {
                @Override
                public void onSuccess(boolean result, TimeoutTask timeoutTask) {
                    if (result) {
                        deleteMemberFromHandleQueue(timeoutTask);
                        return;
                    }
                    executeAfterPerformTaskFail(timeoutTask);
                }

                @Override
                public void onFailure(Exception e, TimeoutTask timeoutTask) {
                    executeAfterPerformTaskFail(timeoutTask);
                }
            });
        }
    }

    private void executeAfterPerformTaskFail(TimeoutTask timeoutTask) {
        timeoutTask.increaseRetryNumber();
        if (timeoutTask.getRetryNumber() >= 16) {
            moveMemberFromHandleQueue2OtherQueue(deadLetterQueue.getQueueType(), timeoutTask);
            return;
        }
        moveMemberFromHandleQueue2OtherQueue(waitingQueue.getQueueType(), timeoutTask);
    }

    private void moveMemberFromHandleQueue2OtherQueue(String moveToQueueType, TimeoutTask timeoutTask) {
        JedisClient.moveMemberFromZSetToZSet(pool,
                calKey(QUEUE_TYPE, bizTag, slotNumber),
                calKey(moveToQueueType, bizTag, slotNumber),
                timeoutTask.calScore(),
                timeoutTask.calValue());
    }

    private void deleteMemberFromHandleQueue(TimeoutTask timeoutTask) {
        try (Jedis jedis = pool.getResource()) {
            jedis.zrem(calKey(QUEUE_TYPE, bizTag, slotNumber), timeoutTask.calValue());
        }
    }
}
