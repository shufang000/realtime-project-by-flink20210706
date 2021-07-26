package com.shufang.flinkapp.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 主要获取一个Jedis的客户端连接对象
 */
public class RedisUtil {

    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            //创建redis的连接池
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100); //最大可用连接数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            jedisPoolConfig.setTestOnBorrow(false); //取消连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnCreate(false);

            jedisPool = new JedisPool(jedisPoolConfig, "shufang102", 6379, 1000);
            System.out.println("开辟连接池");
            return jedisPool.getResource();
        } else {
            System.out.println(" 连接池:" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }

}