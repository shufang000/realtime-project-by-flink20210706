package com.shufang.flinkapp.util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import com.shufang.flinkapp.common.CommonConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * TODO 该类主要通过查询SQL配合反射机制，将Hbase中的维度数据查询出来！
 */
public class PhoenixUtil {

    private static Connection conn = null;

    /**
     * 用来对连接属性进行初始化
     */
    public static void init() {
        try {
            //1 加载驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //2 获取连接
            conn = DriverManager.getConnection(CommonConfig.PHOENIX_URL);
            //设置Schema即命名空间
            conn.setSchema(CommonConfig.HBASE_NAMESPACE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询出一个数据集
     *
     * @param sql   查询的SQL语句
     * @param clazz 反射的类对象
     * @param <T>   最终返回的数据类型的泛型
     * @return List<T>
     */
    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        PreparedStatement ps = null;
        List<T> resultList = new ArrayList<>();

        if (Objects.isNull(conn)) {
            init();
        }
        try {
            //3 获取数据库查询PrepareStatement
            ps = conn.prepareStatement(sql);

            //4 查询出resultSet
            ResultSet resultSet = ps.executeQuery(sql);

            //5 操作ResultSet
            ResultSetMetaData metaData = resultSet.getMetaData();  //元数据
            int columnCount = metaData.getColumnCount(); //列数
            //便利结果集
            while (resultSet.next()) {
                //创建对象
                T obj = clazz.newInstance();

                for (int i = 1; i <= columnCount; i++) {
                    Object value = resultSet.getObject(i);
                    // 由于最终需要返回的是OrderWide，所以不需要下划线转驼峰
                    String columnName = metaData.getColumnName(i);
                    BeanUtils.setProperty(obj, columnName, value);
                }
                //添加到List中
                resultList.add(obj);

            }
            // 关闭资源
            resultSet.close();
            ps.close();

        } catch (Exception throwables) {
            throwables.printStackTrace();
        }

        //6 然后返回查询的结果
        return resultList;
    }

    public static void main(String[] args) {
        List<JSONObject> jsonObjects = queryList("select * from DIM_USER_INFO", JSONObject.class);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
    }
}
