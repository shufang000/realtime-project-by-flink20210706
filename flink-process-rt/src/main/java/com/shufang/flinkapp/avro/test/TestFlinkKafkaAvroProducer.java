package com.shufang.flinkapp.avro.test;

import com.shufang.flinkapp.avro.bean.User;
import com.shufang.flinkapp.avro.schemas.MyAvroSchema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class TestFlinkKafkaAvroProducer {
    private static final String BOOTSTRAP_SERVER = "shufang101:9092,shufang102:9092,shufang103:9092";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        /**
         * 创建User的对象
         */
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        User user2 = new User("Ben", 7, "red");
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setFavoriteColor("blue")
                .setFavoriteNumber(null)
                .build();


        DataStreamSource<User> sourceDS = streamEnv.fromElements(user1, user2, user3);

        String topic = "test_avro";
        // kafka producer的相关配置
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 设置ACK应答为1，可以选择-1，0(all)
        producerConfig.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 1000 * 60 + ""); //超时时长为15分钟
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // 创建Kafka的生产者
        FlinkKafkaProducer<User> producer = new FlinkKafkaProducer<>(topic, new MyAvroSchema(), producerConfig);

        sourceDS.print("avro = ");

        sourceDS.addSink(producer);

        streamEnv.execute();

    }
}
