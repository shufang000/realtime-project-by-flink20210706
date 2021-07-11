package com.shufang.flinkapp.common;

public class CommonConfig {
    // hbase的公共命名空间
    public static final String HBASE_NAMESPACE = "HBASE_SHUFANG";

    // phoenix的连接地址
    public static final String PHOENIX_URL = "jdbc:phoenix:shufang101,shufang102,shufang103:2181";
    // phoenix的连接地址
    public static final String MYSQL_URL = "jdbc:mysql://shufang101:3306/realtime_config?characterEncoding=utf-8&useSSL=false";

}
