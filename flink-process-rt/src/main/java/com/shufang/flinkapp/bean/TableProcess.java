package com.shufang.flinkapp.bean;


import lombok.*;

/**
 * 这是Mysql中的配置表的实体类，用来建立ORM映射关系
 * 下一步创建工具类：MySQLUtil
 */

public class TableProcess {

    // 动态分流的配置常量
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_CLICKHOUSE = "clickhouse";
    //来源表
    String	sourceTable;

    @Override
    public String toString() {
        return "TableProcess{" +
                "sourceTable='" + sourceTable + '\'' +
                ", operateType='" + operateType + '\'' +
                ", sinkType='" + sinkType + '\'' +
                ", sinkTable='" + sinkTable + '\'' +
                ", sinkColumns='" + sinkColumns + '\'' +
                ", sinkPk='" + sinkPk + '\'' +
                ", sinkExtend='" + sinkExtend + '\'' +
                '}';
    }

    public static String getSinkTypeKafka() {
        return SINK_TYPE_KAFKA;
    }

    public static String getSinkTypeHbase() {
        return SINK_TYPE_HBASE;
    }

    public static String getSinkTypeClickhouse() {
        return SINK_TYPE_CLICKHOUSE;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    public String getOperateType() {
        return operateType;
    }

    public void setOperateType(String operateType) {
        this.operateType = operateType;
    }

    public String getSinkType() {
        return sinkType;
    }

    public void setSinkType(String sinkType) {
        this.sinkType = sinkType;
    }

    public String getSinkTable() {
        return sinkTable;
    }

    public void setSinkTable(String sinkTable) {
        this.sinkTable = sinkTable;
    }

    public String getSinkColumns() {
        return sinkColumns;
    }

    public void setSinkColumns(String sinkColumns) {
        this.sinkColumns = sinkColumns;
    }

    public String getSinkPk() {
        return sinkPk;
    }

    public void setSinkPk(String sinkPk) {
        this.sinkPk = sinkPk;
    }

    public String getSinkExtend() {
        return sinkExtend;
    }

    public void setSinkExtend(String sinkExtend) {
        this.sinkExtend = sinkExtend;
    }

    public TableProcess() {
    }

    public TableProcess(String sourceTable, String operateType, String sinkType, String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        this.sourceTable = sourceTable;
        this.operateType = operateType;
        this.sinkType = sinkType;
        this.sinkTable = sinkTable;
        this.sinkColumns = sinkColumns;
        this.sinkPk = sinkPk;
        this.sinkExtend = sinkExtend;
    }

    //操作类型 insert、update、delete
    String	operateType;
    //输出类型：hbase or kafka
    String	sinkType;
    //输出的hbase表（kafka主题）
    String	sinkTable;
    //需要输出的列字段
    String	sinkColumns;
    //输出的主键
    String	sinkPk;
    //输出的拓展信息
    String	sinkExtend;

}
