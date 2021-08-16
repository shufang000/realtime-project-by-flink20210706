package com.shufang.flinkapp.app.dws;

import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.bean.VisitorStats;
import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

/**
 * @访客主题宽表计算 因为单位时间内 mid 的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * 所以用常用统计的四个维度进行聚合 渠道、新老用户、app 版本、省市区域
 * 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10 秒
 * 各个数据在维度聚合前不具备关联性 ，所以 先进行维度聚合
 * 进行关联 这是一个 fulljoin
 * 可以考虑使用 flinksql 完成
 * <p>
 * dwd_page_log
 * dwm_page_jump_dtl
 * dwm_unique_visit
 * 这里的思想是通过Stream.union的方式进行流的合并
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(3);
        // 2 checkpoint
        /*streamEnv.enableCheckpointing(3000);
        streamEnv.getCheckpointConfig().setCheckpointTimeout(60*1000);
        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        streamEnv.setStateBackend(new FsStateBackend(""));*/
        // 3 读取数据源
        DataStreamSource<String> pageDS =
                streamEnv.addSource(KafkaUtil.getConsumer("dwd_page_log", "VisitorStatsAppGroup"));
        DataStreamSource<String> pageJumpDS =
                streamEnv.addSource(KafkaUtil.getConsumer("dwm_page_jump_dtl", "VisitorStatsAppGroup"));
        DataStreamSource<String> uniqueVisitDS =
                streamEnv.addSource(KafkaUtil.getConsumer("dwm_unique_visit", "VisitorStatsAppGroup"));

        //pageDS.print();
        //pageJumpDS.print();
        //uniqueVisitDS.print();

        // 4 进行数据转换，将日志数据转换成统计对象VisitorStats
        // 获取PV页面访问流
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pageDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 1L, 0L, 0L, jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });
        // 获取页面跳出统计流
        SingleOutputStreamOperator<VisitorStats> pageJumpStatsDS = pageJumpDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        0L, 0L, 0L, 1L, jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });
        // 获取UV统计流
        SingleOutputStreamOperator<VisitorStats> UvStatsDS = uniqueVisitDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(jsonStr);
                return new VisitorStats(
                        "", "",
                        jsonObject.getJSONObject("common").getString("vc"),
                        jsonObject.getJSONObject("common").getString("ch"),
                        jsonObject.getJSONObject("common").getString("ar"),
                        jsonObject.getJSONObject("common").getString("is_new"),
                        1L, 0L, 0L, 0L, jsonObject.getJSONObject("page").getLong("during_time"),
                        jsonObject.getLong("ts"));
            }
        });
        // 获取会话统计流
        SingleOutputStreamOperator<VisitorStats> SvStatsDS = pageDS.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String jsonStr, Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                if (lastPageId == null || lastPageId.length() == 0) {
                    // System.out.println("sc:"+json);
                    VisitorStats visitorStats = new VisitorStats("", "",
                            jsonObj.getJSONObject("common").getString("vc"),
                            jsonObj.getJSONObject("common").getString("ch"),
                            jsonObj.getJSONObject("common").getString("ar"),
                            jsonObj.getJSONObject("common").getString("is_new"),
                            0L, 0L, 1L, 0L, 0L, jsonObj.getLong("ts"));
                    out.collect(visitorStats);
                }
            }
        });

        // 5 将5条流进行Union
        DataStream<VisitorStats> unionStatsDS = pvStatsDS.union(UvStatsDS, pageJumpStatsDS, SvStatsDS);

        // 6 指定WaterMark
        SingleOutputStreamOperator<VisitorStats> unionStatsWithTsDS = unionStatsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner((visitorStats, ts) -> visitorStats.getTs())
        );

        // 7 进行分组,按照 vc,ch,ar,is_new
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedStatsDS =
                unionStatsWithTsDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return new Tuple4<>(
                                visitorStats.getVc(),
                                visitorStats.getCh(),
                                visitorStats.getAr(),
                                visitorStats.getIs_new());
                    }
                });

        // TODO 8 开窗，且进行统计,因为返回的数据类型不需要变化，直接使用reduce算子进行计算
        SingleOutputStreamOperator<VisitorStats> reduceStatsDS = keyedStatsDS.window(TumblingEventTimeWindows.of(Time.seconds(10))).reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        //把度量数据两两相加
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                },
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4,
                                        Context context, Iterable<VisitorStats> visitorStatsIn, Collector<VisitorStats> out) throws Exception {
                        for (VisitorStats visitorStats : visitorStatsIn) {
                            String startDate = sdf.format(new Date(context.window().getStart()));
                            String endDate = sdf.format(new Date(context.window().getEnd()));
                            // 将窗口的开始时间、结束时间配置在对象中
                            visitorStats.setStt(startDate);
                            visitorStats.setEdt(endDate);
                            // 输出元素
                            out.collect(visitorStats);
                        }
                    }
                }
        );

        // 9 输出到Olap数据库，ClickHouse
        /*
         * ClickHouse单表查询很快，但是连表查询的时候优势并不明显，甚至不如GP
         * ClickHouse不支持大量的QPS，因为ClickHouse为单条SQL尽量使用到所有的CPU资源
         * partition -> index粒度 ，每个粒度交给一个CPU去执行，充分利用CPU资源
         * partition -> index粒度
         * ......
         * 如果同时有很多SQL一起执行，那么每隔SQL都会对节点上的CPU进行竞争，导致上下文切换严重，所以ClickHouse并不是想象中那么厉害
         */
        reduceStatsDS.print();
        streamEnv.execute();
    }
}
