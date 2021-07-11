package com.shufang.flinkapp.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.app.udf.BaseDBSplitProcessFunction;
import com.shufang.flinkapp.bean.TableProcess;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * 本类主要将ods中从Mysql业务库同步过来的CDC的数据动态分流到不同的dwd介质中
 * -1、 如果是事实数据，我们就还是清洗之后放入到Kafka的dwd层
 * -2、 如果是维度数据，我们就放入到Hbase中，方便通过rowKey进行快速查询
 * -3、 具体的决定数据去向的配置数据存储在MySQL数据库中，可以每隔5s查询一次
 */
public class OdsBaseDBToDwdApp {
    public static void main(String[] args) throws Exception {
        // TODO 0 必须配置一个Hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "shufang");

        // TODO 1 创建执行环境，并进行配置
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1 设置并行度,与kafka的
        streamEnv.setParallelism(3);
        //1.2 设置checkpoint配置
        streamEnv.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.3 设置StateBackEnd
        streamEnv.setStateBackend(
                new FsStateBackend("hdfs://shufang101:9000/flink20210704/checkpoint/odsBaseDBToDwdApp"));

        // TODO 2 从kafka中拉取数据流
        //2.1 通过工具获取KafkaConsumer
        String topic = "ods_base_db";
        String groupId = "baseDBGroup";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = KafkaUtil.getConsumer(topic, groupId);
        flinkKafkaConsumer.setStartFromEarliest();
        //flinkKafkaConsumer.setStartFromLatest();
        flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true); //相当于set enable.auto.commit = false;
        //2.1 获取到数据流
        DataStreamSource<String> jsonStrDs = streamEnv.addSource(flinkKafkaConsumer, "db_source");

        // TODO 3 将无效数据进行清洗，进行ETL，首先将string类型的json字符串转换成Json对象，然后过滤掉“data”:{}的数据 & 长度小于3的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDs.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> cleanJsonDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                boolean isPushDown = value.getString("table") != null && value.getJSONObject("data") != null && value.getString("data").length() >= 3;
                return isPushDown;
            }
        });
        //cleanJsonDS.print();


        // TODO 4 将获取到的数据进行动态的分流，需要读取Mysql中的配置数据，我们选择processFunction
        //  Note:事实数据输出到主流，维度数据写入到Hbase
        OutputTag<JSONObject> dim_hbase_sink = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};
        SingleOutputStreamOperator<JSONObject> factKafkaDS = cleanJsonDS.process(new BaseDBSplitProcessFunction(dim_hbase_sink));
        DataStream<JSONObject> dimHbaseDS = factKafkaDS.getSideOutput(dim_hbase_sink);

        factKafkaDS.print("kafka = ");
        dimHbaseDS.print("hbase = ");



        // TODO
        streamEnv.execute("db_source_split");

    }
}
