package com.wildgoose.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author 张翔
 * @date 2023/3/8 09:38
 * @description
 */
public class JedisUtil {

    private static JedisPool jedisPool;

    private static void initJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig,"dev3",6379,10000,"default","123456");
    }
    public static Jedis getJedis(){
        if(jedisPool == null){
            initJedisPool();
        }
// 获取Jedis客户端
        Jedis jedis = jedisPool.getResource();
        return jedis;
    }

}
