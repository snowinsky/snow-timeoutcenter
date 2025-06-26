package com.snow.al.timeoutcenter;

import lombok.Data;
import lombok.NoArgsConstructor;
import redis.clients.jedis.resps.Tuple;

@Data
@NoArgsConstructor
public class TimeoutTask {

    private static final String OTC = "OTC";

    private String taskFrom;
    private String taskFromId;
    private long taskTimeout;
    private int retryNumber;

    public void increaseRetryNumber() {
        retryNumber++;
    }

    public double calScore() {
        long a = getTaskTimeout() * 1000 + getRetryNumber();
        return (double) a;
    }

    public static String calKey(String queueType, int slotNumber) {
        return OTC + ":" + queueType.toUpperCase() + ":" + "{SLOT" + slotNumber + "}";
    }

    public String calValue() {
        return getTaskFrom() + "__" + getTaskFromId();
    }

    public TimeoutTask(Tuple tuple) {
        String value = tuple.getElement();
        double score = tuple.getScore();
        try {
            String[] a = value.split("__", 2);
            String taskFrom = a[0];
            String taskFromId = a[1];
            this.taskFrom = taskFrom;
            this.taskFromId = taskFromId;
        } catch (Exception e) {
            throw new IllegalArgumentException("member value cannot split by _ to taskFrom and taskTo");
        }

        try {
            double d = score;
            long l = (long) d;
            String lstring = String.valueOf(l);
            String retryString = lstring.substring(lstring.length() - 3);
            String timeoutString = lstring.substring(0, lstring.length() - 3);
            this.retryNumber = Integer.parseInt(retryString);
            this.taskTimeout = Long.parseLong(timeoutString);
        } catch (Exception e) {
            throw new IllegalArgumentException("score value cannot split by 000 to taskTimeout and retryNumber");
        }
    }
}
