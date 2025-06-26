package com.snow.al.timeoutcenter;

public interface DeadLetterHandleFactory {

    void handleTimeoutTask(TimeoutTask timeoutTask);
}
