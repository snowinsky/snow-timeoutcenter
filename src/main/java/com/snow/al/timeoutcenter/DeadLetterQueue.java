package com.snow.al.timeoutcenter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public abstract class DeadLetterQueue implements TimeoutQueue {

    private final DeadLetterHandleFactory deadLetterHandleFactory;

    public static final String QUEUE_TYPE = "DeadLetter";

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
            TimeoutTask a = poll();
            if (a == null) {
                continue;
            }
            log.error("process the dead letter task:{}", a);
            deadLetterHandleFactory.handleTimeoutTask(a);
        }
    }

    @Override
    public void shutdown() {
        isStart = false;
    }
}
