package com.shufang.flinkapp.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Properties;

/**
 * 该工具类主要是用来创建对应的KafkaConsumer和kafkaProducer
 * 让主体的App代码显得更加简洁，便于维护
 */
public class KafkaUtil {

    // 定义消费的kafka的集群的ip及端口
    private static final String BOOTSTRAP_SERVER = "shufang101:9092,shfuang102:9092,shufang103:9092";

    /**
     * 获取到FlinkKafkaConsumer
     *
     * @param topic   ：我们需要消费的topic
     * @param groupId ：我们消费数据的消费者组
     * @return 一个FlinkKafkaConsumer的实例
     */
    public static FlinkKafkaConsumer<String> getConsumer(String topic, String groupId) {

        // 配置Kafka的配置选项，这个可以通过PropertiesConfiguration从配置文件获取，当然还有很多其它的方法
        Properties consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_DOC, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), consumerProps);
    }


    /**
     * 封装一个FlinkKafkaProducer<String>的对象
     *
     * @param topic 需要将消息传入到哪个主题？
     * @return 返回一个FlinkKafkaProducer<String>的对象
     */
    public static FlinkKafkaProducer<String> getProducer(String topic) {

        // 默认是At-least-once语义，当然你可以使用不同的构造器通过 Semantic.EXACTLY_ONCE,来指定端到端的精准一次消费
        /*
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.ACKS_CONFIG, new Integer(1));// 设置producer的ack传输配置
        producerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, Time.hours(2));//设置超市时长，默认1小时，建议1个小时以上
        */

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(BOOTSTRAP_SERVER, topic, new SimpleStringSchema());

        return flinkKafkaProducer;

    }

}
