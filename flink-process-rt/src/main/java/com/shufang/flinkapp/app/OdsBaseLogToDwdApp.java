package com.shufang.flinkapp.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 该Flink的应用类的功能如下：
 * 1、新老用户访问状态的修复
 * 2、消费kafka的数据进行分流操作
 * 3、将不同的主题数据写入到kafka的dwd层
 */
public class OdsBaseLogToDwdApp {
    // 定义dwd需要传递数据的主题
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        // TODO 0、配置HDFS的使用用户，如果需要使用FsStateBackEnd,必须指定一个有读写权限的用户，当前使用
        System.setProperty("HADOOP_USER_NAME", "shufang");


        // TODO 1、首先获取Flink流式执行环境，设置并行度：设置成Kafka的当前消费的topic的偏移量
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(4);

        // 1.1 开启checkpoint保证整个flink处理过程中的 EXACTLY_ONCE的语义
        streamEnv.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(60000); //设置checkpoint超时时长为1分钟
        streamEnv.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);

        /*streamEnv.setStateBackend(
                //状态后端存储在HDFS的指定路径："hdfs://shufang101:9000/flink20210704/checkpoint/odsBaseLogToDwdApp"
                new FsStateBackend("hdfs://shufang101:9000/flink20210704/checkpoint/odsBaseLogToDwdApp"));*/

        // TODO 2、从kafka拉取数据获取数据流
        String topic = "ods_base_log";
        String groupId = "baseLogGroup";
        FlinkKafkaConsumer<String> flinkKafkaConsumer = KafkaUtil.getConsumer(topic, groupId);
        flinkKafkaConsumer.setStartFromLatest();
        //flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true); //相当于set enable.auto.commit = false;
        DataStreamSource<String> baseLogDS = streamEnv.addSource(flinkKafkaConsumer);

        // TODO 3、类型转换 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = baseLogDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSON.parseObject(value);
            }
        });


        // TODO 4、 新老用户的访问状态的修复，由于为每个mid维护一个状态，那么我们需要使用mid(设备id)来进行分组，
        //  并为其维护一个`KeyedState`中的
        //本身客户端业务有新老用户的标识(is_new)，但是不够准确，需要用实时计算再次确认(不涉及业务操作，只是单纯的做个状态确认)。
        /*{
            "common": {
                    ...
                    "is_new": "0",
                    ...
        }, ...
        思路：将每个用户的的首次 访问日期使用状态保存起来，每来一条数据用当前数据的用户访问日期与当前的时间信息做对比，
        如果之前当前访问时间 = 状态存储的时间，那么就不是新用户，反之就是老用户，
        将有误差的“is_new”:“1” 改成 “is_new”:“0”，然后将数据传输到下游
        */
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream =
                jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> fixIsNewDS = jsonObjectStringKeyedStream.map(
                // TODO 为了维护状态，需要使用RichMapFunction
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义状态
                    private ValueState<String> firstMidVisitState;
                    private SimpleDateFormat sf;

                    // 为了给状态ValueState和SimpleDateFormat做初始化操作，我们在open()方法中完成
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        ValueStateDescriptor<String> firstMidVisitState =
                                new ValueStateDescriptor<>("firstMidVisitState", String.class);
                        this.firstMidVisitState = getRuntimeContext().getState(firstMidVisitState);

                        sf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject value) throws Exception {

                        // 获取当前mid是否为新用户
                        String isNew = value.getJSONObject("common").getString("is_new");
                        // 获取当前mid的访问时间时间戳
                        Long ts = value.getLong("ts");

                        // 只修复不准确的新访客
                        if ("1".equals(isNew)) {
                            // 获取当前mid的状态
                            String visitDateState = firstMidVisitState.value();

                            // 获取当前消息的访问时间
                            String curDate = sf.format(new Date(ts));

                            // 如果进了if说明在当天不是新用户
                            if (visitDateState != null && visitDateState.length() != 0) {

                                //判断是否为同一天访问
                                //如果为同一天访问，那么就得将该记录的is_new改成0，然后返回
                                if (visitDateState.equals(curDate)) {
                                    isNew = "0";
                                    value.getJSONObject("common").put("is_new", isNew);
                                }

                            } else {
                                // 如果状态访问日期为空，说明状态中没有该mid的访问记录，那么我们将其状态curDate进行保存
                                firstMidVisitState.update(curDate);
                            }

                        }
                        return value;
                    }
                }
        );


        // TODO 5、使用侧输出流进行分流操作,目前有3种日志类型，start,display,page,使用ProcessFunction来处理侧输出流,最终以String类型
        //  传入到kafka中，方便使用StringSerializer进行序列化，不然序列化比较难弄
        // NOTE：new OutputTag<String>("start") {}中的`{}`必须添加，不然类型不能识别通过~
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = fixIsNewDS.process(new ProcessFunction<JSONObject, String>() {

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                JSONObject start = value.getJSONObject("start");
                // 最终将String类型的对象传输到下一层的kafka层（dwd）
                String jsonString = value.toString();


                // 如果当前jsonObj中包含“start“的属性，那么就输出到测输出流"startTag"
                if (start != null && start.size() > 0) {
                    ctx.output(startTag, jsonString);
                } else { // 不是启动日志的话，进else

                    JSONArray display = value.getJSONArray("displays");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (display != null && display.size() > 0) {
                        /**************************************************/
                        // 因为是Json数组，我们得将数据打散，结合页面id数据进行输出
                        for (int i = 0; i < display.size(); i++) {
                            JSONObject disObj = display.getJSONObject(i);
                            disObj.put("page_id",pageId);
                            ctx.output(displayTag,disObj.toString());
                        }
                        /**************************************************/
                    } else
                        out.collect(jsonString);

                }

            }
        });


        // TODO 6、获取测输出流，主流分别输出到dwd层kafka的对应的主题,addSink(SinkFunction)
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        startDS.addSink(KafkaUtil.getProducer(TOPIC_START));
        displayDS.addSink(KafkaUtil.getProducer(TOPIC_DISPLAY));
        pageDS.addSink(KafkaUtil.getProducer(TOPIC_PAGE));


        // 设置APP的名字，方便发现问题及时定位~~
        streamEnv.execute(OdsBaseLogToDwdApp.class.getName());
    }

}
