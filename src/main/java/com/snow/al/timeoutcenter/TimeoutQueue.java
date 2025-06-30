package com.snow.al.timeoutcenter;

public interface TimeoutQueue {

    String getQueueType();

    String getBizTag();

    boolean add(TimeoutTask timeoutTask);

    boolean del(TimeoutTask timeoutTask);

    void start();

    void shutdown();


}
