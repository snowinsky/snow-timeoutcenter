package com.snow.al.timeoutcenter.redis.jedis.async;

import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.TimeoutTask;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
@Slf4j
public class AsyncHandleFactory {
    private final HandleFactory handleFactory;
    private final ExecutorService executorService;

    public void asyncHandle(TimeoutTask timeoutTask, AsyncHandleListener callback) {
        executorService.execute(() -> {
            try {
                boolean result = handleFactory.performTask(timeoutTask);
                callback.onSuccess(result, timeoutTask);
            } catch (Exception e) {
                callback.onFailure(e, timeoutTask);
            }
        });
    }
}
