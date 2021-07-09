package com.shufang.flinkapp.util;

import com.google.common.base.CaseFormat;
import com.mysql.jdbc.Driver;
import com.shufang.flinkapp.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 从MySQL中查询的通用的工具类，不限定查哪张表
 * ORM
 * 表  ->   Class
 * 字段 ->  属性
 * 一行 ->  对象
 */
public class MySQLUtil {


    /**
     * 查询的数据封装成一个对象的List
     *
     * @param sql               传入的查询的SQL语句
     * @param clz               传入的类模板，用于做反射
     * @param underScoreToCamel 实现需要实现下划线转驼峰，在ORM的时候
     * @return List<T> ,T就是返回的类型
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        ArrayList<T> resultList = new ArrayList<>();
        try {
            // 1 注册驱动
            Class.forName("com.mysql.jdbc.Driver");

            // 2 创建连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://shufang101:3306/realtime_config?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "888888"
            );

            // 3 创建数据库操作对象
            ps = conn.prepareStatement(sql);

            // 4 执行SQL语句
            rs = ps.executeQuery();

            // 5 处理结果集
            ResultSetMetaData metaData = rs.getMetaData();

            // 遍历集合中的元素
            while (rs.next()) {
                // 用于封装一条查询出来的结果数据
                T instance = clz.newInstance();
                int columnCount = metaData.getColumnCount();
                for (int i = 1; i < columnCount + 1; i++) {
                    //获取第`i`列的列名称
                    String columnLabel = metaData.getColumnName(i);
                    String propName = columnLabel;
                    //将表中的列（下划线）转换成类属性的驼峰命名法的形式,使用google的【guava】工具包
                    if (underScoreToCamel) {
                        // 拿到驼峰命名
                        propName = CaseFormat.LOWER_UNDERSCORE.converterTo(CaseFormat.LOWER_CAMEL).convert(columnLabel);
                    }
                    // 调用apache的commons-beans工具包给对象属性赋值
                    BeanUtils.setProperty(instance,propName,rs.getObject(i));
                }

                // 将每层循环的封装的对象放入到需要被返回的List中
                resultList.add(instance);
            }

            // 7 最终返回封装的对象List
            return resultList;


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Mysql查询数据失败！~");
        } finally {
            // 6 释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
