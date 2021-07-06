package com.shufang.flinkapp.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestValueState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("shufang101", 9999);
        SingleOutputStreamOperator<Tuple2<String, String>> stream1 = source.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split(",");

                return new Tuple2<>(split[0], split[1]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> vStateDS = stream1.keyBy(a -> a.f0)
                .map(new RichMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

                    private ValueState<String> vState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vState = getRuntimeContext().getState(new ValueStateDescriptor<String>("vState", String.class));
                    }

                    @Override
                    public Tuple2<String, String> map(Tuple2<String, String> value) throws Exception {

                        String oldState = vState.value();
                        System.out.println("old state = " + oldState);

                        vState.update(value.f1);
                        String newState = vState.value();
                        System.out.println("new state = " + newState);
                        return value;
                    }
                });

        /** 控制台打印的日志如下  ~~~~，切记KeyedStream为每个Key维护了一个状态，如果不是KeyedStream只能维护一种状态，那就是List状态
         * 2021-07-04 23:42:52,190 WARN [org.apache.flink.runtime.webmonitor.WebMonitorUtils] - Log file environment variable 'log.file' is not set.
         * 2021-07-04 23:42:52,191 WARN [org.apache.flink.runtime.webmonitor.WebMonitorUtils] - JobManager log files are unavailable in the web dashboard. Log file location not found in environment variable 'log.file' or configuration key 'web.log.path'.
         * old state = null
         * new state = a
         * vStateDS:23> (shufang,a)
         * old state = a
         * new state = b
         * vStateDS:23> (shufang,b)
         * old state = b
         * new state = c
         * vStateDS:23> (shufang,c)
         */
        vStateDS.print("vStateDS");


        env.execute("value");
    }
}
