package com.shufang.flinkapp.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

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
    public static JSONObject queryDimWithOutCache(String tableName, Tuple2<String, String>... columnAndValues) throws Exception {
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
        if (!Objects.isNull(jsonObjects) || jsonObjects.size() > 0) {
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
    public static JSONObject queryDimWithCache(String tableName, Tuple2<String, String>... columnAndValues) throws Exception {
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
        if (!Objects.isNull(jsonObjects) || jsonObjects.size() > 0) {
            //由于Hbase中的rowkey对应的数据只可能有1条，所以通过rowkey查询出来的数据只可能size = 1
            resultDim = jsonObjects.get(0);
        } else {
            System.out.println("没有找到对应的维度数据" + sql);
        }
        //5 最终返回我们需要的数据，这相当于一个点查数据
        return resultDim;
    }

    //因为通常按照id查询的次数很多，所以封装一个专门通过id主键查询的
    public static JSONObject queryDimWithCache(String tableName, String id) throws Exception {
        return queryDimWithCache(tableName, Tuple2.of("id", id));
    }


    public static void main(String[] args) throws Exception {
        JSONObject dim_user_info = queryDimWithOutCache("DIM_USER_INFO", Tuple2.of("id", "10001"));
        System.out.println(dim_user_info);
    }
}
