package com.shufang.flinkapp.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Objects;

public class DimQueryUtil {

    /**
     * 这是一个普通的查询维度的方法，每来一条数据都会去Hbase中查询匹配对应的维度！！通常不可取，还有优化空间
     *
     * @param tableName       查询的表名
     * @param columnAndValues 用来拼接SQL的WHERE KEY = VALUE的参数
     * @return 将查询的结果以JSONObject对象的方式返回
     * @throws Exception 如果传入参数的个数为<1，那么就报异常，至少按照rowkey进行查询
     */
    public static JSONObject queryDimWithOutCache(
            String tableName, Tuple2<String, String>... columnAndValues) throws Exception {
        Objects.requireNonNull(columnAndValues);
        if (columnAndValues.length < 1) {
            throw new Exception("至少传入一个主键参数，但是目前的参数个数为0");
        }
        //1 拼接查询语句：SELECT * FROM TABELNAME WHERE RK = XX AND XXXXX = XXXXXX;
        String whereExpress = " where ";
        for (int i = 0; i < columnAndValues.length; i++) {
            Tuple2<String, String> columnAndValue = columnAndValues[i];
            if (i > 0) {
                whereExpress += " and ";
            }
            whereExpress += columnAndValue.f0 + "= '" + columnAndValue.f1 + "'";
        }
        //2 拼接SQL完成
        String sql = "SELECT * FROM " + tableName + whereExpress;

        //3 调用PhoenixUtil的查询方法
        JSONObject resultDim = null;
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql, JSONObject.class);

        //4 校验并返回结果
        if (jsonObjects.size() > 0) {
            //由于Hbase中的rowkey对应的数据只可能有1条，所以通过rowkey查询出来的数据只可能size = 1
            resultDim = jsonObjects.get(0);
        } else {
            System.out.println("没有找到对应的维度数据" + sql);
        }
        //5 最终返回我们需要的数据，这相当于一个点查数据
        return resultDim;
    }

    /**
     * TODO 优化1：通过旁路缓存redis的方式进行缓存的优化，
     * 1、首先去查redis，如果匹配到结果则返回
     * 2、如果查询的结果为null，那么再去查hbase，并且将hbase的查询的结果添加到redis中
     * 注意：必须设置TTL过期时间，防止冷数据占用资源;要考虑维度数据是否会发生变化，如果发生变化要主动清除缓存
     *
     * @param tableName       查询的表名
     * @param columnAndValues 用来拼接SQL的WHERE KEY = VALUE的参数
     * @return 将查询的结果以JSONObject对象的方式返回
     * @throws Exception 如果传入参数的个数为<1，那么就报异常，至少按照rowkey进行查询
     */
    public static JSONObject queryDimWithCache(
            String tableName, Tuple2<String, String>... columnAndValues) throws Exception {
        Objects.requireNonNull(columnAndValues);
        if (columnAndValues.length < 1) {
            throw new Exception("至少传入一个主键参数，但是目前的参数个数为0");
        }
        //1 拼接Phoenix查询语句
        // TODO Redis的Key设计：dim:tableName:value_value2...
        String whereExpress = " where ";
        String redisKey = "";
        for (int i = 0; i < columnAndValues.length; i++) {
            Tuple2<String, String> columnAndValue = columnAndValues[i];
            if (i > 0) {
                whereExpress += " and ";
                // redis key part
                redisKey += "_";
            }
            redisKey += columnAndValue.f1;
            whereExpress += columnAndValue.f0 + "= '" + columnAndValue.f1 + "'";
        }

        //2 现在Redis中查询数据，如果没有去Phoenix中查询，并返回结果放入到redis，设置超时时间，Redis的数据类型选择String
        Jedis jedis = null;
        String dimDataStr = null;
        JSONObject dimJsonObj = null;

        String key = "dim:" + tableName.toLowerCase() + ":" + redisKey;
        try {
            jedis = RedisUtil.getJedis();
            dimDataStr = jedis.get(key);
        } catch (Exception e) {
            System.out.println("reids缓存异常：" + key);
            e.printStackTrace();
        }
        // 如果查到了数据，那么
        if (!Objects.isNull(dimDataStr) && dimDataStr.length() > 0) {
            dimJsonObj = JSONObject.parseObject(dimDataStr);
        } else {
            //如果没有查到数据，然后利用Phoenix的查询SQL去Hbase中查询
            //2 拼接SQL完成
            String sql = "SELECT * FROM " + tableName + whereExpress;
            //3 调用PhoenixUtil的查询方法
            List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql, JSONObject.class);
            //4 校验并返回结果
            if (jsonObjects.size() > 0) {
                //由于Hbase中的rowkey对应的数据只可能有1条，所以通过rowkey查询出来的数据只可能size = 1
                dimJsonObj = jsonObjects.get(0);

                //将查询的结果添加到Redis
                if (!Objects.isNull(jedis)) {
                    jedis.setex(key, 12 * 3600, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("没有找到对应的维度数据" + sql);
            }
        }
        // 关闭缓存连接
        if (!Objects.isNull(jedis)) {
            jedis.close();
        }

        return dimJsonObj;
    }

    //因为通常按照id查询的次数很多，所以封装一个专门通过id主键查询的
    public static JSONObject queryDimWithCache(String tableName, String id) throws Exception {
        return queryDimWithCache(tableName, Tuple2.of("id", id));
    }

    //TODO 如果当前处理的维度数据时update产生的数据，那么得手动清空Redis中的缓存，这个需要在处理数据的时候就做，类DimHbaseSink的invoke
    public static void deleteCache(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key); //删除无效缓存
            System.out.println("删除Redis已变更的缓存完成：key = " + key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("redis删除缓存时缓存异常！！");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        /*JSONObject dim_user_info = queryDimWithCache(
                "DIM_USER_INFO", Tuple2.of("id", "10001"), Tuple2.of("login_name", "4bwf7meg1u2"));
        System.out.println(dim_user_info);
        JSONObject dim_user_info1 = queryDimWithCache("DIM_USER_INFO", Tuple2.of("id", "10001"));
        System.out.println(dim_user_info1);*/



        deleteCache("sdewr","sdas");
    }
}
