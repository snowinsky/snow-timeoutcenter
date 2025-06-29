# snow-timeoutcenter
----

> This is a timeout center for common tasks.
> 

You can use it by the below code.

~~~java
JedisPool pool = JedisClient.getJedisPool();
int slotNumber = 1;

SnowTimeoutCenter snowTimeoutCenter = new RedisJedisTimeoutCenter(pool, slotNumber);
snowTimeoutCenter.start();

var threadPool = Executors.newFixedThreadPool(4);

for (int i = 0; i < 50; i++) {
    TimeoutTask tt = new TimeoutTask();
    tt.setTaskFrom("AABSCCC");
    tt.setTaskFromId("sdfdsfasdf" + System.nanoTime());
    tt.setTaskTimeout(System.currentTimeMillis() + i);

    threadPool.execute(()->snowTimeoutCenter.publish(tt));
}

threadPool.shutdown();
~~~