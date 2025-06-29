package com.snow.al.timeoutcenter;

import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.Optional;
import java.util.concurrent.TimeoutException;

@RequiredArgsConstructor
public abstract class HandleQueue implements TimeoutQueue {

    public static final String QUEUE_TYPE = "Handle";

    @Setter
    protected WaitingQueue waitingQueue;
    protected final HandleFactory handleFactory;
    protected final DeadLetterQueue deadLetterQueue;
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
            TimeoutTask timeoutTask = poll();
            if (timeoutTask == null) {
                continue;
            }
            try {
                boolean executeResult = handleFactory.performTask(timeoutTask);
                if (!executeResult) {
                    timeoutTask.increaseRetryNumber();
                    if (timeoutTask.getRetryNumber() >= 16) {
                        deadLetterQueue.add(timeoutTask);
                        continue;
                    }
                    Optional.ofNullable(waitingQueue).ifPresent(a -> a.add(timeoutTask));
                }
            } catch (TimeoutException e) {
                timeoutTask.increaseRetryNumber();
                if (timeoutTask.getRetryNumber() >= 16) {
                    deadLetterQueue.add(timeoutTask);
                    continue;
                }
                Optional.ofNullable(waitingQueue).ifPresent(a -> a.add(timeoutTask));
            }
        }
    }

    @Override
    public void shutdown() {
        isStart = false;
    }
}
