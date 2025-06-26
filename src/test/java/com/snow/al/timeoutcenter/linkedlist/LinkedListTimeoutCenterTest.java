package com.snow.al.timeoutcenter.linkedlist;

import com.snow.al.timeoutcenter.TimeoutTask;
import junit.framework.TestCase;

public class LinkedListTimeoutCenterTest extends TestCase {

    public void testInitDeadLetterQueue() {

        LinkedListTimeoutCenter ltc = new LinkedListTimeoutCenter();
        ltc.start();

        for (int i = 0; i < 50; i++) {
            TimeoutTask tt = new TimeoutTask();
            tt.setTaskFrom("aaa");
            tt.setTaskFromId("aaa_" + i);
            tt.setTaskTimeout(System.currentTimeMillis() + 1000 + i);

            ltc.publish(tt);

        }

        ltc.shutdown();
    }
}