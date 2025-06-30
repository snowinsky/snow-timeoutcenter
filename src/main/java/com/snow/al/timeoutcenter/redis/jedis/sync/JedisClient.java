package com.snow.al.timeoutcenter.redis.jedis.sync;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.resps.Tuple;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
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

    public static JedisPool getJedisPool() {
        return pool;
    }

    public static Tuple moveTimeoutMemberFrom2To(JedisPool pool, String fromKey, String toKey) {
        String script = "local msg = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')\n" +
                "if not msg[1] then return nil end\n" +
                "if tonumber(msg[2]) > tonumber(ARGV[1]) then return nil end\n" +
                "local zrem_ret = redis.call('ZREM', KEYS[1], msg[1])\n" +
                "if zrem_ret ~= 1 then return nil end\n" +
                "local zadd_ret = redis.call('ZADD', KEYS[2], tonumber(msg[2]), msg[1])\n" +
                "if zadd_ret ~= 1 then return nil end\n" +
                "return {msg[1], msg[2]}";
        List<String> keys = List.of(fromKey, toKey);
        List<String> values = List.of("" + System.currentTimeMillis() * 1000);
        try (Jedis jedis = pool.getResource()) {
            Object o = jedis.eval(script, keys, values);
            if (o != null) {
                List<String> l = (List<String>) o;
                if (l.size() != 2) {
                    throw new IllegalStateException("eval failed");
                }
                String member = l.get(0);
                String score = l.get(1);
                return new Tuple(member, Double.parseDouble(score));
            }
        }
        return null;
    }

    public static Tuple moveMemberFrom2To(JedisPool pool, String fromKey, String toKey, String fromValue, String newScore) {
        String script = "local score = redis.call('ZSCORE', KEYS[1], ARGV[1])\n" +
                "if not score then return nil end\n" +
                "local zrem_ret = redis.call('ZREM', KEYS[1], ARGV[1])\n" +
                "if zrem_ret ~= 1 then return nil end\n" +
                "local zadd_ret = redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])\n" +
                "if zadd_ret ~= 1 then return nil end\n" +
                "return {ARGV[1], score}";
        List<String> keys = List.of(fromKey, toKey);
        List<String> values = List.of(fromValue, newScore);
        try (Jedis jedis = pool.getResource()) {
            Object o = jedis.eval(script, keys, values);
            if (o != null) {
                List<String> l = (List<String>) o;
                if (l.size() != 2) {
                    throw new IllegalStateException("eval failed");
                }
                String member = l.get(0);
                String score = l.get(1);
                return new Tuple(member, Double.parseDouble(score));
            }
        }
        return null;
    }

    public static Long moveTimeoutMembersFrom2To(JedisPool pool, String fromKey, String toKey, long timeout, TimeUnit unit) {
        try (Jedis jedis = pool.getResource()) {
            String script = "local msgs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES', 'LIMIT', 0, 10000)\n" +
                    "for i = 1, #msgs, 2 do\n" +
                    "    if redis.call('ZREM', KEYS[1], msgs[i]) == 1 then\n" +
                    "        redis.call('ZADD', KEYS[2], msgs[i+1], msgs[i])\n" +
                    "    end\n" +
                    "end\n" +
                    "return #msgs";
            List<String> keys = List.of(fromKey, toKey);
            List<String> values = List.of("" + (System.currentTimeMillis() - unit.toMillis(timeout)));
            Object o = jedis.eval(script, keys, values);
            return Optional.ofNullable(o).map(Object::toString).map(Long::parseLong).orElse(0L);
        }
    }


    public static void main(String[] args) {

        try (Jedis jedis = pool.getResource()) {
            for (int i = 0; i < 5; i++) {
                jedis.zadd("testkey1", System.currentTimeMillis() + i, "jack" + System.nanoTime());
            }
        }

        try (Jedis jedis = pool.getResource()) {
            String script = "local msgs = redis.call('ZRANGEBYSCORE', KEYS[1], 0, ARGV[1], 'WITHSCORES', 'LIMIT', 0, 10000)\n" +
                    "for i = 1, #msgs, 2 do\n" +
                    "    if redis.call('ZREM', KEYS[1], msgs[i]) == 1 then\n" +
                    "        redis.call('ZADD', KEYS[2], msgs[i+1], msgs[i])\n" +
                    "    end\n" +
                    "end\n" +
                    "return #msgs";
            List<String> keys = List.of("testkey1", "testkey2");
            List<String> values = List.of("" + System.currentTimeMillis() + 1000);
            Object o = jedis.eval(script, keys, values);
            if (o == null)
                log.info("o:{}", o);
        }


    }

}
