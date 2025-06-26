package com.snow.al.timeoutcenter.linkedlist;

import com.snow.al.timeoutcenter.*;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

@Slf4j
public class LinkedListTimeoutCenter extends AbstractTimeoutCenterFacade {

    @Override
    protected DeadLetterQueue initDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory) {
        return new LinkedListDeadLetterQueue(deadLetterHandleFactory);
    }

    @Override
    protected HandleQueue initHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue) {
        return new LinkedListHandleQueue(handleFactory, deadLetterQueue);
    }

    @Override
    protected WaitingQueue initWaitingQueue(HandleQueue handleQueue) {
        return new LinkedListWaitingQueue(handleQueue);
    }

    @Override
    protected DeadLetterHandleFactory initSingletonDeadLetterHandleFactory() {
        return timeoutTask -> log.info("receive a dead letter message:{}", timeoutTask);
    }

    @Override
    protected HandleFactory initSingletonHandleFactory() {
        return timeoutTask -> {
            int a = ThreadLocalRandom.current().nextInt(1, 4);
            switch (a) {
                case 1:
                    log.info("process the task true:{}", timeoutTask);
                    return true;
                case 2:
                    log.info("process the task false:{}", timeoutTask);
                    return false;
                case 3:
                    log.info("process the task timeout:{}", timeoutTask);
                    throw new TimeoutException("handle task timeout");
            }
            log.error("sssssssssssssssss");
            return false;
        };
    }
}
