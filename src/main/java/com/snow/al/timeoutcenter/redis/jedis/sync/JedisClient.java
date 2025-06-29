package com.snow.al.timeoutcenter.redis.jedis.sync;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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

    private static String moveMemberFromZSetToZSetLuaSha = "";

    private static final String MOVE_MEMBER_FROM_ZSET_TO_ZSET_LUA = "local del_cnt = redis.pcall('ZREM', KEYS[1], KEYS[4])\n" +
            "local add_cnt = 0\n" +
            "if del_cnt > 0 then\n" +
            "    add_cnt = redis.pcall('zadd', KEYS[2], KEYS[3], KEYS[4])\n" +
            "end\n" +
            "return add_cnt";

    private static final String MOVE_MEMBER_FROM_ZSET_TO_ZSET_LUA_ = "redis.pcall('ZREM', KEYS[1], KEYS[4])\n" +
            "return redis.pcall('zadd', KEYS[2], KEYS[3], KEYS[4])\n";

    public static String getMoveMemberFromZSetToZSetLuaSha(JedisPool pool) {
        try (Jedis jedis = pool.getResource()) {
            if (moveMemberFromZSetToZSetLuaSha.isEmpty()) {
                moveMemberFromZSetToZSetLuaSha = jedis.scriptLoad(MOVE_MEMBER_FROM_ZSET_TO_ZSET_LUA_);
                return moveMemberFromZSetToZSetLuaSha;
            }
            boolean has = jedis.scriptExists(moveMemberFromZSetToZSetLuaSha);
            if (has) {
                return moveMemberFromZSetToZSetLuaSha;
            } else {
                moveMemberFromZSetToZSetLuaSha = jedis.scriptLoad(MOVE_MEMBER_FROM_ZSET_TO_ZSET_LUA_);
                return moveMemberFromZSetToZSetLuaSha;
            }
        }
    }

    public static boolean moveMemberFromZSetToZSet(JedisPool pool, String fromKey, String toKey, String score, String member) {
        try (Jedis jedis = pool.getResource()) {
            String sha = getMoveMemberFromZSetToZSetLuaSha(pool);
            Object ret = jedis.evalsha(sha, 4, fromKey, toKey, score, member);
            if (ret instanceof Long) {
                return true;
            }
            throw new IllegalStateException("move member from zset to another one failed");
        }
    }

    public static boolean moveMemberFromZSetToZSet(JedisPool pool, String fromKey, String toKey, double score, String member) {
        return moveMemberFromZSetToZSet(pool, fromKey, toKey, String.valueOf(score), member);
    }

}
