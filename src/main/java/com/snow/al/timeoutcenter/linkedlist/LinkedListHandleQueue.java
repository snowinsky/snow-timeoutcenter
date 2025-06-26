package com.snow.al.timeoutcenter.linkedlist;

import com.snow.al.timeoutcenter.*;

import java.util.LinkedList;

public class LinkedListHandleQueue extends HandleQueue {

    private final LinkedList<TimeoutTask> lh = new LinkedList<>();

    public LinkedListHandleQueue(HandleFactory handleFactory, DeadLetterQueue deadLetterQueue) {
        super(handleFactory, deadLetterQueue);
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        return lh.add(timeoutTask);
    }

    @Override
    public TimeoutTask peek() {
        return lh.peek();
    }

    @Override
    public TimeoutTask poll() {
        return lh.poll();
    }
}
