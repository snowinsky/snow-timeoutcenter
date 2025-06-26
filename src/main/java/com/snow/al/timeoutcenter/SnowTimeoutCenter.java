package com.snow.al.timeoutcenter;

public interface SnowTimeoutCenter {

    void start();

    void shutdown();

    boolean publish(TimeoutTask timeoutTask);
}
