package com.snow.al.timeoutcenter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class WaitingQueue implements TimeoutQueue {

    public static final String QUEUE_TYPE = "Waiting";

    protected final HandleQueue handleQueue;
    private volatile boolean isStart;

    @Override
    public String getQueueType() {
        return QUEUE_TYPE;
    }

    @Override
    public void start() {
        isStart = true;
        while (true) {
            if (!isStart) {
                return;
            }
            TimeoutTask timeoutTask = peek();
            if (timeoutTask == null) {
                continue;
            }
            if (TimeLongUtil.currentTimeMillis() >= timeoutTask.getTaskTimeout()) {
                moveTaskFromWaitingQueueToHandleQueue(timeoutTask);
            }
        }
    }

    protected boolean moveTaskFromWaitingQueueToHandleQueue(TimeoutTask timeoutTask) {
        return handleQueue.add(poll());
    }

    @Override
    public void shutdown() {
        isStart = false;
    }

}
