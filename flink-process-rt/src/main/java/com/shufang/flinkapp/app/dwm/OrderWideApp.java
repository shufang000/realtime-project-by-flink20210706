package com.shufang.flinkapp.app.dwm;

import com.alibaba.fastjson.JSON;
import com.shufang.flinkapp.bean.OrderDetail;
import com.shufang.flinkapp.bean.OrderInfo;
import com.shufang.flinkapp.bean.OrderWide;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Objects;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);
        //TODO 生产需要设置checkpoint

        // 2 定义需要消费的主题
        String orderInfoTopic = "dwd_order_info";
        String orderDtlInfoTopic = "dwd_order_detail";
        String groupId = "order_wide_group";
        // 定义输出的DWM的主题：dwm_order_wide
        String sinkTopic = "dwm_order_wide";


        // 3 读取OrderInfo数据
        FlinkKafkaConsumer<String> orderInfoConsumer = KafkaUtil.getConsumer(orderInfoTopic, groupId);
        orderInfoConsumer.setStartFromEarliest();
        SingleOutputStreamOperator<OrderInfo> orderInfoDS
                = streamEnv.addSource(orderInfoConsumer)
                .map(new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat sdf = null; //字符串与日期以及时间戳之间的转换

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        System.out.println("sdf = " + sdf);
                        // 将json字符串中的属性与实体类的属性进行映射，生成需要的OrderInfo对象
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        // 给时间戳字段赋值，这个用于后续的EventTime的WaterMark的指定
                        orderInfo.setCreate_ts(sdf.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                });

        // 4 读取OrderDetail的数据
        FlinkKafkaConsumer<String> orderDtlConsumer = KafkaUtil.getConsumer(orderDtlInfoTopic, groupId);
        orderDtlConsumer.setStartFromEarliest();
        SingleOutputStreamOperator<OrderDetail> orderDtlDS
                = streamEnv.addSource(orderDtlConsumer)
                .map(new RichMapFunction<String, OrderDetail>() {

                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        orderDetail.setCreate_ts(sdf.parse(orderDetail.getCreate_time()).getTime());
                        return orderDetail;
                    }
                });
        //orderInfoDS.print("order ==");
        //orderDtlDS.print("orderdtl ==");

        // 5.指定时间字段与watermark
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderInfo>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }
                ));
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDtlDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        }
                )
        );

        //6 TODO 使用interval join进行双流的join，其实还有窗口join，在flink sql中还有 regular join 以及 temporal join(dim关联)
        KeyedStream<OrderInfo, Long> orderInfoKeyedStream = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailKeyedStream = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedStream.intervalJoin(orderDetailKeyedStream)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(orderInfo, orderDetail));
                    }
                });
        //orderWideDS.print();

        // 7 关联维度数据，维度数据存在Hbase中，引入旁路缓存Redis而不是堆缓存，增强稳定性


        streamEnv.execute();
    }
}