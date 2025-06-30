package com.snow.al.timeoutcenter;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class HandleQueue implements TimeoutQueue {

    public static final String QUEUE_TYPE = "Handle";

    protected volatile boolean isStart;

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
            startCore();
            try {
                Thread.sleep(30000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract void startCore();

    @Override
    public void shutdown() {
        isStart = false;
    }
}
