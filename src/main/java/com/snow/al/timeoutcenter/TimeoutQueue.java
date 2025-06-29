package com.snow.al.timeoutcenter;

public interface TimeoutQueue {

    String getQueueType();

    String getBizTag();

    boolean add(TimeoutTask timeoutTask);

    TimeoutTask peek();

    TimeoutTask poll();

    void start();

    void shutdown();


}
