package com.ql.kafka.java;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author: wautumnli
 * @date: 2021-01-28 14:14
 **/
public class MsgProducer {

    public static void main(String[] args) {

        // 设置配置文件
        Properties props = new Properties();

        // 设置服务启动器
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // acks确认
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        // 发送失败重试 重试次数
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // 重试时间间隔 默认是100ms
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 300);
        // 设置发送消息的本地缓冲区 提高发射性能 默认33554432 即是 32MB
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        // 设置批量发送消息的大小 默认是16384 即是 16kb
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 设置消息需要存储多久发送
        // 如果是0 即是立即发送 非常影响性能
        // 一般是100ms左右 如果100ms没到消息满足Batch Size也发送，如果到了100ms消息没有满足Batch Size也发送
        props.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        // key的序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value的序列化配置
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        Order order;
        System.out.println("Producer..");
        for (int i = 0; i < 30; i++) {
            order = new Order(i, "orderType:" + i);
            // 4个参数 1。topic名字 2。分区号 3。key 4。value
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>
                    ("kafka-test", 0, order.getId().toString(), JSON.toJSONString(order));

            producer.send(producerRecord, (recordMetadata, e) -> {
                if (e != null) {
                    System.out.println("消息发送失败");
                }

                if (recordMetadata != null) {
                    System.out.println("消息发送成功: " + recordMetadata.topic() + " " + recordMetadata.partition() + " " + recordMetadata.offset());
                }
            });
        }

        producer.close();

    }
}
