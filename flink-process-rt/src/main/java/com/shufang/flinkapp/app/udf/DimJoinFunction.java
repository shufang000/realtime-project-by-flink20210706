package com.shufang.flinkapp.app.udf;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * 这是一个接口，指定维度查询的条件key，同时进行事实与维度的关联，将其交给具体的调用方去实现，
 * 这叫做：“模板方法设计模式”
 */
public interface DimJoinFunction<T> {
    /**
     * 获取到查询Hbase中维度数据的keu
     * @param t 流中的数据
     * @return 查询维度的key
     */
    String getKey(T t);

    /**
     * 事实与维度数据关联的方法
     * @param t 事实数据
     * @param jsonObject 维度数据
     */
    void join(T t, JSONObject jsonObject) throws ParseException, Exception;

}
