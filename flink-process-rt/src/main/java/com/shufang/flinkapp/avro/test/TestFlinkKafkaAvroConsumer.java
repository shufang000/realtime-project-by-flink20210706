package com.shufang.flinkapp.avro.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.shufang.flinkapp.avro.bean.User;
import com.shufang.flinkapp.avro.bean.User1;
import com.shufang.flinkapp.avro.schemas.MyAvroSchema;

import com.shufang.flinkapp.util.KafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class TestFlinkKafkaAvroConsumer {
    private static final String BOOTSTRAP_SERVER = "shufang101:9092,shufang102:9092,shufang103:9092";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "test_avro";
        String groupId = "avroTestGroup";

        String deserClass = "com.shufang.flinkapp.avro.schemas.MyAvroSchema.java";

        // 配置Kafka的配置选项，这个可以通过PropertiesConfiguration从配置文件获取，当然还有很多其它的方法
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyAvroSchema.class.getName());


        FlinkKafkaConsumer<User1> consumer = new FlinkKafkaConsumer<>(topic, new MyAvroSchema(), consumerProps);
        consumer.setStartFromEarliest();

        DataStreamSource<User1> userDS = streamEnv.addSource(consumer);

        /*
         * User = :3> {"name": "Alyssa", "favorite_number": 256, "favorite_color": null}
         * User = :4> {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
         * User = :2> {"name": "Charlie", "favorite_number": null, "favorite_color": "blue"}
         */
        //userDS.print("User = ");

        SingleOutputStreamOperator<String> user1DS = userDS.map(new MapFunction<User1, String>() {
            @Override
            public String map(User1 user1) throws Exception {
                return user1.toString();
            }
        });

        /*
         * 3> {"name": "Alyssa", "favorite_number": 256, "favorite_color": null}
         * 4> {"name": "Ben", "favorite_number": 7, "favorite_color": "red"}
         * 2> {"name": "Charlie", "favorite_number": null, "favorite_color": "blue"}
         */
        user1DS.print();

        user1DS.addSink(KafkaUtil.getProducer("test_json"));
        streamEnv.execute();

    }
}
