package com.snow.al.timeoutcenter;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.resps.Tuple;

@Slf4j
public class TimeoutTaskTest extends TestCase {

    public void testCalScore() {
        TimeoutTask t = new TimeoutTask(new Tuple("aaa_bbb0001", 12414141420012D));
        log.info("{}", t);
    }

    public void testCalValue() {
        TimeoutTask t = new TimeoutTask(new Tuple("aaa_bbb0001", 12414141426712D));
        log.info("{}", t);
    }
}