package com.shufang.flinkapp.app.udf;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.common.CommonConfig;
import com.shufang.flinkapp.util.DimQueryUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimHbaseSinkFunction extends RichSinkFunction<JSONObject> {

    Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //TODO 初始化连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(CommonConfig.PHOENIX_URL);
        System.out.println("Phoenix连接" + conn + "初始化完毕");
    }

    //TODO 实现维度数据插入到Hbase的操作
    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {

        // 1 获取到SinkObject的sinkTable
        String sinkTable = jsonObject.getString("sink_table");
        JSONObject dataJsonObj = jsonObject.getJSONObject("data");

        // 2 然后拼接upsertSQL
        Set<String> columns = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        //TODO 这里使用apache common的StringUtils工具类进行SQL的拼接
        String columnsString = StringUtils.join(columns, ",");
        String valuesStr = StringUtils.join(values, "','");
        String upsertSQL = "UPSERT INTO " + CommonConfig.HBASE_NAMESPACE + "." + sinkTable.toUpperCase() + "(" +
                columnsString +
                ") VALUES ('" + valuesStr + "')";

        // 3 执行SQL
        PreparedStatement ps = null;

        try {
            System.out.println("upsertSQL ====" +upsertSQL);
            ps = conn.prepareStatement(upsertSQL);
            ps.execute();
            conn.commit(); //Phoenix的JDBC默认不会自动提交事务，需要手动提交，这里与MySQL的不一样
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            //关闭PrepareStatement
            if (ps != null) {
                ps.close();
            }
        }
        // TODO 如果为update操作，那么就删除Redis中失效的缓存，否则数据会不一致
        if (jsonObject.getString("type").equals("update")){
            DimQueryUtil.deleteCache(sinkTable,dataJsonObj.getString("id"));
        }

    }
}
