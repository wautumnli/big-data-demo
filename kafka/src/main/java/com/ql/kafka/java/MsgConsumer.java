package com.ql.kafka.java;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author: wautumnli
 * @date: 2021-01-28 15:06
 **/
public class MsgConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置连接
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 设置消费组
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_group");

        // 设置自动提交时间
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        // 设置是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // 设置心跳时间 broker通过心跳时间确认consumer是否还存活
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
        // broker多久感知不到consumer 会将consumer踢出群组
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10 * 1000);
        // 如果两次poll时间超过这个 broker会认为这个consumer消费能力太差 将该consumer踢出群组
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30 * 1000);
        // key 序列化
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // value 序列化
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        String topic = "kafka-test";
        // 订阅相应主题分区
        consumer.assign(Collections.singletonList(new TopicPartition(topic, 0)));

        while (true) {

            ConsumerRecords<String, String> consumerRecord = consumer.poll(Duration.ofDays(1));
            for (ConsumerRecord<String, String> record : consumerRecord) {
                System.out.printf("收到消息, offset: %d, count: %d, key: %s, value: %s%n", record.offset(), consumerRecord.count(),record.key(), record.value());
            }
            // 是否有消费
            if (consumerRecord.count() > 0) {
                try {
                    // 异步提交任务
                    consumer.commitAsync();
                }catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
