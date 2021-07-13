package com.shufang.flinkapp.app.udf;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.bean.TableProcess;
import com.shufang.flinkapp.common.CommonConfig;
import com.shufang.flinkapp.util.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class BaseDBSplitProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    private OutputTag<JSONObject> outputTag;
    // TODO 0 KV的类型为 =>> <表明:操作 -> TableProcess>,用于存储配置信息的缓存
    private Map<String, TableProcess> configMap = new HashMap<String, TableProcess>(32);
    // 用于存放第一次初始化已经创建了的表，下一次定时读取更新后，只需要创建没有处理过的表了，这里使用一个Set集合进行存储
    private Set<String> finishedTables = new HashSet<String>();

    public BaseDBSplitProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    // 声明Phoenix的连接对象
    Connection conn = null;

    /**
     * 在这个初始化的方法里创建定时器，每5秒执行一次MySQL的配置数据
     *
     * @param parameters 参数
     * @throws Exception 异常
     */
    @Override
    public void open(Configuration parameters) throws Exception {


        // TODO 1 获取到Phoenix的连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(CommonConfig.PHOENIX_URL);


        // TODO 2 初始化配置信息
        refreshMetaFromMysql();

        // TODO 3 由于MySQL中的配置信息可能会变化，所以开启一个定时任务，每隔一段时间（5s）查询MySQL并更新Map：configMap中的配置信息
        //  从现在起，在delay5000ms及之后，每隔5000ms执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMetaFromMysql();
            }
        }, 5000, 5000);
    }


    // TODO 开始处理数据，在读取完配置表之后，对数据进行分流处理
    @Override
    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {

        // 1、jsonObj为数据流中的一个元素，首先我们从该数据中拼接出我们想要的key，然后去Map中寻找该key的sink信息
        String tableName = jsonObj.getString("table");
        String type = jsonObj.getString("type");

        // TODO 2、 注意：先修复数据，针对使用Maxwell的Bootstrap同步历史数据的时候，type会变成bootstrap-insert而不是insert
        if ("bootstrap-insert".equals(type)) {
            type = "insert";
            jsonObj.put("type", type);
        }

        //3、拼接key，找到sink配置信息，并将sinkColumn之外的jsonObj中的信息从该Json中剔除
        if (configMap != null || configMap.size() > 0) {
            // 进行configMap的key的拼接
            String key = tableName + ":" + type;
            // 获取到该表的数据的sink的配置信息
            TableProcess tableProcess = configMap.get(key);

            // 如果获取到的信息不为null,我们对json的sinkColumns进行处理
            if (tableProcess != null) {
                // 给Json添加SinkTable的标签信息：如 “sinkTable”:“dim_xxxxxx”
                jsonObj.put("sink_table", tableProcess.getSinkTable());

                // 获取到sinkColumns，如果不为空，我们需要提出sinkColumn以外的数据
                String sinkColumns = tableProcess.getSinkColumns();

                if (sinkColumns != null && sinkColumns.length() > 0) {
                    //创建一个方法对sinkColumns进行过滤,通过【迭代器】将`"data":{}`不在sinkColumns中的json的kv数据剔除
                    filterSinkColumns(jsonObj.getJSONObject("data"), sinkColumns);
                }

                //TODO 将json数据进行处理之后，我们需要通过sinkType将数据进行分流处理
                if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                    // 如果sinkType = hbase，也就是维度数据，就往hbase的流中发送
                    ctx.output(outputTag, jsonObj);
                } else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)) {
                    // 如果sinkType = kafka，也就是事实数据，就往kafka的的主流中发送
                    out.collect(jsonObj);
                }

            } else {
                //System.out.println("该Map中没有获取到该key" + key + "的配置信息，请检查应用是否重启");

            }
        }


    }

    /**
     * 将dataJsonObj中不在sinkColumns的KV数据进行删除
     *
     * @param dataJsonObj 传入的json数据
     * @param sinkColumns 传入的sinkColumns数据
     */
    private void filterSinkColumns(JSONObject dataJsonObj, String sinkColumns) {

        // 1 首先将sinkColumns转换成集合类型
        String[] cols = sinkColumns.split(",");
        List<String> sinkCols = Arrays.asList(cols);

        //TODO 需要使用迭代器删除Json中的对应的Entry，而不能使用for循环
        Set<Map.Entry<String, Object>> entries = dataJsonObj.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();

        //TODO 使用迭代器的特性删除不需要的节点
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!sinkCols.contains(next.getKey())) {
                //如果不在sinkColumns中的数据进行删除！！！
                iterator.remove();
            }
        }
    }

    /**
     * 从Mysql中查询配置表的信息
     */
    private void refreshMetaFromMysql() {
        //================1、读取我们的配置表======================================================
        System.out.println("查询分流配置表信息，并使用MySQLUtil工具类进行ORM映射，并且缓存在一个内存中的Map中" + System.currentTimeMillis());
        String sql = "select * from table_process";
        List<TableProcess> tableProcesses = MySQLUtil.queryList(sql, TableProcess.class, true);

        for (TableProcess tableProcess : tableProcesses) {
            // 获取表明，与json中的”tablename“进行对应
            String sourceTable = tableProcess.getSourceTable();
            // 获取操作类型
            String operateType = tableProcess.getOperateType();
            // 输出类型
            String sinkType = tableProcess.getSinkType();
            // 输出的主键
            String sinkPk = tableProcess.getSinkPk();
            // 输出的目标表
            String sinkTable = tableProcess.getSinkTable();
            // 需要被输出的列
            String sinkColumns = tableProcess.getSinkColumns();
            // 输出的拓展信息
            String sinkExtend = tableProcess.getSinkExtend();

            String finalKey = sourceTable + ":" + operateType;

            //================2、更新我们的内存中的配置信息==========================================
            configMap.put(finalKey, tableProcess);

            //================3、检查Hbase中是否有配置信息中的表，没有的话就帮忙创建一下==================
            //====方便将维度数据放进去的时候Hbase中已经有表了，这里使用Phoenix操作========================
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)
                    && "insert".equals(operateType)
                    && !finishedTables.contains(sinkTable)) {
                //如果为insert，sinkType = hbase ,且set集合中不存在tableName，并且创建之后将其添加进set集合
                //如果返回true，说明这个table没有被处理过，set中也不存在，此时add方法添加成功，如果false，则标识已经存在，添加失败！！
                boolean notExisted = finishedTables.add(sinkTable);

                // TODO 如果不存在，也不能保证hbase中就没有这个表(可能应用重启)，所以还需要进一步的check,并在check之后创建表
                if (notExisted) {
                    checkTableAndCreate(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }

            }
        }
        //TODO 最后校验是否从MySQL中获取到数据，初始化的配置信息获取以及Hbase中的表就已经做好了准备了
        if (configMap == null || configMap.size() == 0) {
            throw new RuntimeException("没有从MySQL数据库中获取到配置数据，请检查~~");
        }

    }

    private void checkTableAndCreate(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        // 处理pk与sinkExtend
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }

        // 拼接建表语句,使用StringBuffer
        StringBuffer ddl = new StringBuffer("create table if not exists " +
                CommonConfig.HBASE_NAMESPACE + "." + sinkTable + "(");

        String[] fields = sinkColumns.split(",");
        for (int i = 0; i < fields.length; i++) {
            //如果是主键字段，添加 primary key的约束语句,如果不是就不需要加
            if (sinkPk.equals(fields[i])) {
                ddl.append(fields[i]).append(" varchar primary key ");
            } else {
                ddl.append("info.").append(fields[i]).append(" varchar ");
            }

            //如果是最后一个字段，就不添加",",如果不是就添加","
            if (i != fields.length - 1) {
                ddl.append(",");
            }
        }

        // 拼接)与见表的扩展语句
        ddl.append(") ");
        ddl.append(sinkExtend);


        //TODO 在拼接好建表语句之后，需要获取一个Phoenix连接，来执行建表语句，为了减少创建的次数，我们在Open方法中创建连接
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(ddl.toString());
            ps.execute();
            System.out.println("执行SQL ：【" + ddl + "】建表语句成功");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close(); //关闭ps，conn不需要关闭，因为这个连接是一个长连接
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                    throw new RuntimeException("Phoenix 建表失败！~~~~~~");
                }
            }
        }
    }

}
