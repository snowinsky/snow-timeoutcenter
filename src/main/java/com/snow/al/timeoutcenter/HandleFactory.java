package com.snow.al.timeoutcenter;

import java.util.concurrent.TimeoutException;

public interface HandleFactory {

    boolean performTask(TimeoutTask timeoutTask) throws TimeoutException;
}
