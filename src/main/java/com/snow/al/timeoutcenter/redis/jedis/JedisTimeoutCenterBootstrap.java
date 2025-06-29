package com.snow.al.timeoutcenter.redis.jedis;

import com.snow.al.timeoutcenter.DeadLetterHandleFactory;
import com.snow.al.timeoutcenter.HandleFactory;
import com.snow.al.timeoutcenter.SnowTimeoutCenter;
import com.snow.al.timeoutcenter.TimeoutTask;
import com.snow.al.timeoutcenter.redis.jedis.async.AsyncRedisJedisTimeoutCenterBootstrap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.JedisPool;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class JedisTimeoutCenterBootstrap implements SnowTimeoutCenter {

    private final JedisPool jedisPool;
    private final int slotCount;
    private final String bizTag;
    private final DeadLetterHandleFactory deadLetterHandleFactory;
    private final HandleFactory handleFactory;

    private AsyncRedisJedisTimeoutCenterBootstrap timeoutCenterBootstrap;

    @Override
    public void start() {
        var aa = new AsyncRedisJedisTimeoutCenterBootstrap(jedisPool, bizTag, slotCount,
                deadLetterHandleFactory, handleFactory);
        aa.start();
        timeoutCenterBootstrap = aa;
    }

    @Override
    public void shutdown() {
        Optional.ofNullable(timeoutCenterBootstrap).ifPresent(AsyncRedisJedisTimeoutCenterBootstrap::shutdown);
    }

    @Override
    public boolean publish(TimeoutTask timeoutTask) {
        if (timeoutCenterBootstrap == null) {
            log.error("async redis jedis timeout center is not started....");
            return false;
        }
        return timeoutCenterBootstrap.publish(timeoutTask);
    }
}
