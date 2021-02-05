package top.tzk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Author: tianzhenkun
 * @Date: 2021/2/3
 * @Description:
 * @Modified By:
 */
public class Producer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafKaEnum.HOST.getValue());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try(KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties)){
            while (true){
                String message = String.valueOf(System.currentTimeMillis());
                ProducerRecord<String,String> record = new ProducerRecord<String, String>(KafKaEnum.TOPIC.getValue(), message);
                producer.send(record);
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
