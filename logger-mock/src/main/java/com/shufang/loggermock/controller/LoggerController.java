package com.shufang.loggermock.controller;

import lombok.extern.slf4j.Slf4j;
import lombok.extern.slf4j.XSlf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;

/**
 * 这是一个接口，用来处理其它接口发送过来的数据
 * - 1、我们可以将数据输出到控制台
 * - 2、可以输出到Kafka
 * - 3、可以罗落盘到本地磁盘
 *     - 对于日志的落盘，我们可以采用主流的日志框架如：@logback、@log4j来完成，当前采用logback
 */
@RestController
@Slf4j //该注解等同于，Logger log = LoggerFactory.getLogger(LoggerController.class);
public class LoggerController {

    @Autowired
    KafkaTemplate kafkaTemplate;
    //private static final Logger log = LoggerFactory.getLogger(LoggerController.class);

    /*别人请求的时候需要通过
     * https://localhost:8081/applog?param=hellothisisamotherfucker
     * 的方式进行请求你这个接口
     * */
    @RequestMapping("/applog")
    public String processLog(@RequestParam("param") String jsonLog){

        // 在该方法中处理你接受到的jsonLog，可以按照以下方式进行日志的处理
        //1 在控制台打印
        //System.out.println(jsonLog);

        //2 落盘到磁盘
        log.info(jsonLog);

        //3 发送到kafka
        /*Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"shufang101:9092,shufang102:9092,shufang103:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<>("topic","key",jsonLog));*/

        kafkaTemplate.send("ods_base_log",jsonLog);
        return "success";
    }
}
