package com.shufang.flinkapp.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.app.udf.DimAsyncFunction;
import com.shufang.flinkapp.bean.OrderDetail;
import com.shufang.flinkapp.bean.OrderInfo;
import com.shufang.flinkapp.bean.OrderWide;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
        //orderInfoConsumer.setStartFromEarliest();
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
        //orderDtlConsumer.setStartFromEarliest();
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

        // TODO 7 关联维度数据，维度数据存在Hbase中，引入旁路缓存Redis而不是堆缓存，增强稳定性，这里采用异步IO的方式，提升对外部系统请求的效率
        // 7.1 关联用户维度：DIM_USER_INFO
        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS1 = AsyncDataStream.unorderedWait(
                orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getUser_id()); //确定查询维度的key
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        //TODO 需要实现与用户维度的关联，获取到user_gender\age字段
                        SimpleDateFormat formattor = new SimpleDateFormat("yyyy-MM-dd");
                        String birthday = jsonObject.getString("BIRTHDAY");
                        Date date = formattor.parse(birthday);
                        Long curTs = System.currentTimeMillis();
                        Long betweenMs = curTs - date.getTime();
                        Long ageLong = betweenMs / 1000L / 60L / 60L / 24L / 365L;
                        Integer age = ageLong.intValue();
                        orderWide.setUser_age(age);
                        orderWide.setUser_gender(jsonObject.getString("GENDER"));
                    }
                }, 60000, TimeUnit.MILLISECONDS, 300);



        //7.2 关联省份维度、SKU、SPU等维度，与用户维度都是按照同样的方法
        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS2 = AsyncDataStream.unorderedWait(
                orderWideJoinedDS1, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getProvince_id());
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS3 = AsyncDataStream.unorderedWait(
                orderWideJoinedDS2, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSku_id());
                    }
                }, 60, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS4 = AsyncDataStream.unorderedWait(
                orderWideJoinedDS3, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS5 = AsyncDataStream.unorderedWait(
                orderWideJoinedDS4, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);


        SingleOutputStreamOperator<OrderWide> orderWideJoinedDS6 = AsyncDataStream.unorderedWait(
                orderWideJoinedDS5, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);



        SingleOutputStreamOperator<String> orderWideFinalDS = orderWideJoinedDS6.map(JSON::toJSONString);
        orderWideFinalDS.print("order wide string == ");
        orderWideFinalDS.addSink(KafkaUtil.getProducer(sinkTopic));

        streamEnv.execute();
    }
}