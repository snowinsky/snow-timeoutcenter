package com.snow.al.timeoutcenter;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class WaitingQueue implements TimeoutQueue {

    public static final String QUEUE_TYPE = "Waiting";

    private final HandleQueue handleQueue;
    private volatile boolean isStart;

    @Override
    public abstract boolean add(TimeoutTask timeoutTask);

    @Override
    public abstract TimeoutTask peek();

    @Override
    public abstract TimeoutTask poll();

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
                handleQueue.add(poll());
            }
        }
    }

    @Override
    public void shutdown() {
        isStart = false;
    }

}
