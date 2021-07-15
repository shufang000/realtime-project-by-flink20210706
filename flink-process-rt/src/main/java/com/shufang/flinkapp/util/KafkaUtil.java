package com.shufang.flinkapp.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
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
     * 该方法只能使用最简单的，传入一个Topic，将String类型的Message传入到Kafka的指定的Topic中，不能实现动态的Topic的选择如：
     * 1、粘性分配
     * 2、Hash分配
     * 这取决于你使用FlinkKafkaProducer的哪种构造器，下面一个重载的方法实现动态的Topic-Partition的传输
     *
     * @param topic 需要将消息传入到哪个主题？
     * @return 返回一个FlinkKafkaProducer<String>的对象
     */
    public static FlinkKafkaProducer<String> getProducer(String topic) {
        // 默认是At-least-once语义，当然你可以使用不同的构造器通过 Semantic.EXACTLY_ONCE,来指定端到端的精准一次消费
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>(BOOTSTRAP_SERVER, topic, new SimpleStringSchema());
        return flinkKafkaProducer;

    }


    /**
     * TODO 这是一个通用的方法，可以帮助kafka序列化传入的任意类型的对象
     * @param defaultTopic 如果不指定topic，那么消息就传输到这个默认的topic
     * @param kafkaSerSchema 与SimpleStringSchema雷系，可以自定义
     * @param <T> 需要被序列化的类型
     * @return 返回一个FlinkKafkaProducer
     */
    public static <T> FlinkKafkaProducer<T> getProducer(String defaultTopic, KafkaSerializationSchema<T> kafkaSerSchema) {
        // kafka producer的相关配置
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // 设置ACK应答为1，可以选择-1，0(all)
        kafkaConfig.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15 * 1000 * 60 + ""); //超时时长为15分钟
        kafkaConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);

        // 创建生产者，并返回
        return new FlinkKafkaProducer<T>(
                defaultTopic,
                kafkaSerSchema, //这个序列化的Schema可以选择将消息传输到指定的topic-partition，也可动态round-robin || hash分配消息
                kafkaConfig,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE // 选择精准一次消费
        );

    }

}
