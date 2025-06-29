package com.snow.al.timeoutcenter.linkedlist;

import com.snow.al.timeoutcenter.HandleQueue;
import com.snow.al.timeoutcenter.TimeoutTask;
import com.snow.al.timeoutcenter.WaitingQueue;

import java.util.LinkedList;

public class LinkedListWaitingQueue extends WaitingQueue {

    private final String bizTag;
    private final LinkedList<TimeoutTask> lw = new LinkedList<>();

    public LinkedListWaitingQueue(HandleQueue handleQueue, String bizTag) {
        super(handleQueue);
        this.bizTag = bizTag;
    }

    @Override
    public String getBizTag() {
        return bizTag;
    }

    @Override
    public boolean add(TimeoutTask timeoutTask) {
        return lw.add(timeoutTask);
    }

    @Override
    public TimeoutTask peek() {
        return lw.peek();
    }

    @Override
    public TimeoutTask poll() {
        return lw.poll();
    }
}
