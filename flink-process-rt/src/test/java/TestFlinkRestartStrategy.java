import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.app.udf.BaseDBSplitProcessFunction;
import com.shufang.flinkapp.bean.TableProcess;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

/**
 * TODO 本类主要测试FLink的重启策略，这个重启策略必须配合检查点一起使用
 *  1、当没配置checkpoint，默认的重启策略为 non-restart
 *  2、当配置了checkpoint，flink的task失败后的默认的重启策略为自动重启，重启INTEGER.maxvalue()次
 */
public class TestFlinkRestartStrategy {
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
  /*      streamEnv.setStateBackend(
                new FsStateBackend("hdfs://shufang101:9000/flink20210704/checkpoint/odsBaseDBToDwdApp"));
*/
        streamEnv.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE,3000));

        // TODO 2 从kafka中拉取数据流
        //2.1 通过工具获取KafkaConsumer
        String topic = "ods_base_db";
        String groupId = "baseDBGroup";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = KafkaUtil.getConsumer(topic, groupId);
        //flinkKafkaConsumer.setStartFromLatest();
        //flinkKafkaConsumer.setStartFromLatest();
        //flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true); //相当于set enable.auto.commit = false;
        //2.1 获取到数据流
        DataStreamSource<String> jsonStrDs = streamEnv.addSource(flinkKafkaConsumer, "db_source");
        jsonStrDs.print("转换前=>>");
        // TODO 3 将无效数据进行清洗，进行ETL，首先将string类型的json字符串转换成Json对象，然后过滤掉“data”:{}的数据 & 长度小于3的数据
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDs.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                System.out.println(10/0);
                return JSON.parseObject(value);
            }
        });

        jsonObjDS.print("转换后=>>");


        streamEnv.execute("");


    }
}
