package top.tzk.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/3
 * @Description:
 * @Modified By:
 */
public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafKaEnum.HOST.getValue());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "tzk_kafka");
        try(KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties)){
            consumer.subscribe(Collections.singletonList(KafKaEnum.TOPIC.getValue()));
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
