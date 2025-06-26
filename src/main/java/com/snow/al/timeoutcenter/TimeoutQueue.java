package com.snow.al.timeoutcenter;

public interface TimeoutQueue {

    boolean add(TimeoutTask timeoutTask);

    TimeoutTask peek();

    TimeoutTask poll();

    void start();

    void shutdown();


}
