package com.snow.al.timeoutcenter.linkedlist;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.DeadLetterQueue;
import com.snow.al.timeoutcenter.TimeoutTask;

import java.util.LinkedList;

public class LinkedListDeadLetterQueue extends DeadLetterQueue {

    private final String bizTag;
    private final LinkedList<TimeoutTask> ll = new LinkedList<>();

    public LinkedListDeadLetterQueue(DeadLetterHandleFactory deadLetterHandleFactory, String bizTag) {
        super(deadLetterHandleFactory);
        this.bizTag = bizTag;
    }

    @Override
    public String getBizTag() {
        return bizTag;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        return ll.add(timeoutTask);
    }

    @Override
    public TimeoutTask peek() {
        return ll.peek();
    }

    @Override
    public TimeoutTask poll() {
        return ll.poll();
    }
}
