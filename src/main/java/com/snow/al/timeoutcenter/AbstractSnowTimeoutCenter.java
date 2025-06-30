package com.snow.al.timeoutcenter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSnowTimeoutCenter implements SnowTimeoutCenter{

    private DeadLetterQueue deadLetterQueue;
    private HandleQueue handleQueue;
    private WaitingQueue waitingQueue;

    @Override
    public void start() {
        log.info("===>>>> timeout server will start.....");
        HandleFactory handleFactory = initSingletonHandleFactory();
        DeadLetterHandleFactory deadLetterHandleFactory = initSingletonDeadLetterHandleFactory();
        deadLetterQueue = initDeadLetterQueue(deadLetterHandleFactory);
        handleQueue = initHandleQueue(handleFactory, deadLetterQueue);
        waitingQueue = initWaitingQueue(handleQueue, handleFactory);

        new Thread(() -> deadLetterQueue.start()).start();
        new Thread(() -> handleQueue.start()).start();
        new Thread(() -> waitingQueue.start()).start();
        log.info("===>>>> timeout server started.....");
    }

    @Override
    public boolean publish(TimeoutTask timeoutTask) {
        return waitingQueue.add(timeoutTask);
    }

    protected abstract DeadLetterQueue initDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory);

    protected abstract HandleQueue initHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue);

    protected abstract WaitingQueue initWaitingQueue(HandleQueue handleQueue, HandleFactory handleFactory);

    protected abstract DeadLetterHandleFactory initSingletonDeadLetterHandleFactory();

    protected abstract HandleFactory initSingletonHandleFactory();

    @Override
    public void shutdown() {
        waitingQueue.shutdown();
        handleQueue.shutdown();
        deadLetterQueue.shutdown();
    }
}
