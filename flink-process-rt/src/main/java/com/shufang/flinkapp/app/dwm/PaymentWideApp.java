package com.shufang.flinkapp.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.bean.OrderWide;
import com.shufang.flinkapp.bean.PaymentInfo;
import com.shufang.flinkapp.bean.PaymentWide;
import com.shufang.flinkapp.util.DataTimeFormatUtil;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 生成支付宽表的应用实体类
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);

        //TODO 生产需要设置checkpoint

        // 2 定义需要消费的主题
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String groupId = "payment_wide_group";
        // 定义输出的DWM的主题：dwm_order_wide
        String paymentWideSinkTopic = "dwm_payment_wide";

        // 3 开始创建数据流、指定Watermark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = streamEnv
                .addSource(KafkaUtil.getConsumer(paymentInfoSourceTopic, groupId).setStartFromEarliest()).map(
                        new MapFunction<String, PaymentInfo>() {
                            @Override
                            public PaymentInfo map(String jsonStr) throws Exception {
                                return JSONObject.parseObject(jsonStr, PaymentInfo.class);
                            }
                        }
                ).assignTimestampsAndWatermarks(
                        // 指定WaterMark
                        WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (paymentInfo, ts) -> DataTimeFormatUtil.getTsFromDateStr(paymentInfo.getCallback_time())
                                )
                );
        //paymentInfoDS.print("paymentInfoDS");

        SingleOutputStreamOperator<OrderWide> orderWideDS = streamEnv
                .addSource(KafkaUtil.getConsumer(orderWideSourceTopic, groupId).setStartFromEarliest()).map(
                        new MapFunction<String, OrderWide>() {
                            @Override
                            public OrderWide map(String jsonStr) throws Exception {
                                return JSONObject.parseObject(jsonStr, OrderWide.class);
                            }
                        }
                ).filter(o -> (o.getCreate_time() != null)) //过滤掉脏数据
                .assignTimestampsAndWatermarks(
                        // 指定WaterMark
                        WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(
                                        (orderWide, ts) -> DataTimeFormatUtil.getTsFromDateStr(orderWide.getCreate_time())
                                )
                );

        orderWideDS.print("orderWideDS");

        // 4 指定分组的键
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideDS.keyBy(OrderWide::getOrder_id);

        // 5 进行interval join,假设订单在30分钟之内支付才算有效
        DataStream<PaymentWide> joinedPaymentDS = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS)
                .inEventTime()
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });


        // 6 将关联后的结果以字符串的格式进行输出到kafka-DWM层：dwm_payment_wide
        SingleOutputStreamOperator<String> paymentWideFinalDS = joinedPaymentDS.map(new MapFunction<PaymentWide, String>() {
            @Override
            public String map(PaymentWide value) throws Exception {
                return JSONObject.toJSONString(value);
            }
        });

        paymentWideFinalDS.print("paymentWideFinalDS= ");
       // paymentWideFinalDS.addSink(KafkaUtil.getProducer(paymentWideSinkTopic));

        streamEnv.execute();
    }
}
