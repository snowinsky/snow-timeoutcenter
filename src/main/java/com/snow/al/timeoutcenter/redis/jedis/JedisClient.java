package com.snow.al.timeoutcenter.redis.jedis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.resps.Tuple;

import java.util.List;

public class JedisClient {

    private static final JedisPool pool;

    static {
        JedisPoolConfig jpc = new JedisPoolConfig();
        jpc.setMinIdle(8);
        jpc.setMaxIdle(16);
        jpc.setMaxTotal(128);
        pool = new JedisPool(jpc, "127.0.0.1", 6379);
    }

    public static Jedis getJedis() {
        return pool.getResource();
    }

    public static JedisPool getJedisPool(){
        return pool;
    }

    public static void main(String[] args) {
        try (Jedis jedis = pool.getResource()) {
            List<Tuple> t = jedis.zrangeWithScores("key1", 0, 0);
            System.out.println(t);

            var tt = jedis.zpopmin("key1");
            System.out.println(tt);

            t = jedis.zrangeWithScores("key1", 0, 0);
            System.out.println(t);

        }
    }

}
