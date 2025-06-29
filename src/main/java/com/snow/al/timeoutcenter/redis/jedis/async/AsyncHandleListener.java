package com.snow.al.timeoutcenter.redis.jedis.async;

import com.snow.al.timeoutcenter.TimeoutTask;

public interface AsyncHandleListener {

    void onSuccess(boolean result, TimeoutTask timeoutTask);

    void onFailure(Exception e, TimeoutTask timeoutTask);
}
