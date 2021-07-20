package com.shufang.flinkapp.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * TODO 本类主要从启动日志的kafka topic：dwd_page_log中计算出日活
 *      - 只要last_page_id不为空，且有效那么肯定不是第一次访问
 *      - 在状态中保留每个mid的上次访日期yyyyMMdd，如果当前访问日期 = 上次访问日期，那么就是在一天中访问多次，只算一次unique visitor
 */
public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);

        // 2 定义需要消费的主题
        String topic = "dwd_page_log";
        String groupId = "unique_visitor_group";

        // 3 将String类型的数据转换成JSONObject对象
        FlinkKafkaConsumer<String> consumer = KafkaUtil.getConsumer(topic, groupId);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamEnv.addSource(consumer).map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });

        jsonObjDS.print();
        // 4 为了定义KeyedState，我们先对数据进行keyBy操作
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> uvDS = keyedDS.filter(
                new RichFilterFunction<JSONObject>() {
                    // 用来存储每个mid的上次的访问时间
                    ValueState<String> lastVisDateState = null;
                    // 用来对Long类型的ts进行格式化
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                        ValueStateDescriptor<String> lastVisDateStateDesc = new ValueStateDescriptor<>("lastVisDateState", String.class);

                        // 设置状态超时时间为1day
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        // 每次状态创建或每次写入都更新超时时间的时间戳
                        //.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        // 默认值，一旦过期了，那么永远不会被返回给调用方，只会返回空状态
                        //.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired).build();
                        lastVisDateStateDesc.enableTimeToLive(stateTtlConfig);
                        // TODO 给状态初始化
                        lastVisDateState = getRuntimeContext().getState(lastVisDateStateDesc);
                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        // 1 首先判断当前页面是否有上页标识，如果说有肯定不是当日的第一次访问，所以被过滤掉
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (!Objects.isNull(lastPageId) && lastPageId.length() > 0) {
                            return false;
                        }

                        // 获取当前页面的访问时间curDate和该mid的状态时间lastVisDate，然后做对比
                        Long ts = jsonObject.getLong("ts");
                        String curDate = sdf.format(new Date(ts));
                        String lastVisDate = lastVisDateState.value();
                        // 如果状态中已经存在不为null的值，且该值有效且与当前访问时间相等，那么就说明当日访问过，直接pass，且时间Date不需要更新
                        if (!Objects.isNull(lastVisDate) && lastVisDate.length() > 0 && curDate.equals(lastVisDate)) {
                            System.out.println("【当日活跃】：当日已经访问过!!!!!!，被过滤掉了");
                            return false;
                        } else {
                            // 说明没有访问过，更新状态且返回true
                            System.out.println("【当日活跃】：当日没有访问过~~，更新状态ing");
                            lastVisDateState.update(curDate);
                            return true;
                        }
                    }
                }
        ).uid("UVFilter");

        // 定义输出的DWM的主题：dwm_unique_visit
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaProducer<String> producer = KafkaUtil.getProducer(sinkTopic);
        uvDS.map(JSON::toString).addSink(producer);

        streamEnv.execute();

    }
}
