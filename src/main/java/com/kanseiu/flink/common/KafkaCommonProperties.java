package com.kanseiu.flink.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

import static com.kanseiu.flink.common.KafkaConfig.*;

public class KafkaCommonProperties {

    /**
     * 通用消费者配置
     * @return Properties
     */
    public static Properties getKafkaConsumerProperties(String consumerGroup){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_OFFSET);
        return properties;
    }

    /**
     * 通用生产者配置
     * @return Properties
     */
    public static Properties getKafkaProducerProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "3600000");
        return properties;
    }
}
