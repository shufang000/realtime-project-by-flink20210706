package com.shufang.flinkapp.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 该类主要用于从dwd层dwd_page_log主题读取数据,将跳出明细写入到dwm层，用于dws层页面跳出率的计算 跳出数/访问数
 * 页面跳出规则：
 * 1.用户首次访问页面， 通过last_page_id来识别 last_page_id == null
 * 2.且首次访问之后10s内没有访问过其它的页面 通过CEP来进行匹配
 */
public class PageJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);

        // 2 定义需要消费的主题
        String topic = "dwd_page_log";
        String groupId = "page_jump_group";
        // 定义输出的DWM的主题：dwm_page_jump_dtl
        String sinkTopic = "dwm_page_jump_dtl";

        // 3 将String类型的数据转换成JSONObject对象
        FlinkKafkaConsumer<String> consumer = KafkaUtil.getConsumer(topic, groupId);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = streamEnv.addSource(consumer).map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });

        // 4 配置时间语义，定义Watermark，FLink1.12默认的时间语义是ExactlyOnce
        //streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<JSONObject> jsonObjWithTsDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
        );

        // 5 通过mid进行分组
        KeyedStream<JSONObject, String> keyByJsonDS = jsonObjWithTsDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 6 定义CEP combine pattern
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(
                new SimpleCondition<JSONObject>() {
                    // 条件1：进入的第一个页面
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                        if (Objects.isNull(lastPageId) || lastPageId.length() == 0) {
                            return true;
                        }
                        return false;
                    }
                }).next("next").where(
                new SimpleCondition<JSONObject>() {
                    // 条件2： 10s内该id有访问过第二个页面
                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("page_id");
                        if (!Objects.isNull(pageId) && pageId.length() > 0) {
                            return true;
                        }
                        return false;
                    }
                }
        ).within(Time.milliseconds(10000));//条件3：定义2个简单条件之间允许的最大时间范围，用于通过将超时事件输出到侧输出流，即我们的跳出明细


        // 7 将stream与pattern进行匹配
        PatternStream<JSONObject> patternDS = CEP.pattern(keyByJsonDS, pattern);

        // 8 通过flatSelect方法选取超时数据到侧输出流为跳出明细
        OutputTag<String> jumpTag = new OutputTag<String>("jumpTag") {
        };
        SingleOutputStreamOperator<String> mainDS = patternDS.flatSelect(
                jumpTag,
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                        //进来的都是10s内没有访问其它页面的mid的数据，为跳出数据，会自动打赏outputTag
                        List<JSONObject> jumpedEvent = map.get("start");
                        for (JSONObject jsonObject : jumpedEvent) {
                            collector.collect(jsonObject.toJSONString()); //最终输出
                        }
                    }
                },
                new PatternFlatSelectFunction<JSONObject, String>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                        //因为不超时的事件为正常的跳转，不为跳出时间，所以不提取，所以这里不写代码
                    }
                }
        );

        // TODO 9 获取侧输出流中的超时数据，即跳出明细的数据
        DataStream<String> jumpedDS = mainDS.getSideOutput(jumpTag);


        FlinkKafkaProducer<String> producer = KafkaUtil.getProducer(sinkTopic);
        jumpedDS.print(">>>");
        jumpedDS.addSink(producer);


        streamEnv.execute();

    }
}
