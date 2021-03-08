package top.tzk.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;

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
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            Random random = new Random();
            DecimalFormat format = new DecimalFormat("###0.00");
            while (true) {
                StringBuilder builder = new StringBuilder();
                String temperature = format.format(37 + random.nextGaussian());
                long id = 1001 + random.nextInt(10);
                long timestamp =  Math.round(random.nextGaussian() * 2000) + System.currentTimeMillis();
                builder.append(id).append(",").append(timestamp).append(",").append(temperature);
                String message = builder.toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(KafKaEnum.TOPIC.getValue(), message);
                producer.send(record);
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
