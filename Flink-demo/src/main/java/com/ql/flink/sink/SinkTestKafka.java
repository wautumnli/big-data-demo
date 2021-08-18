package com.ql.flink.sink;

import com.ql.flink.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Properties;

/**
 * @author: wautumnli
 * @date: 2021-08-18 19:19
 **/
public class SinkTestKafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // kafka相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        // 从kafka中获取
        // 第一个参数为topic，第二个是反序列化工具，第三个是kafka连接配置
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<>("sensor", new SimpleStringSchema(), properties));

        DataStream<String> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2])).toString();
        });
        // sink到kafka
        dataStream.addSink(new FlinkKafkaProducer011<>("localhost:9092", "sinkTest", new SimpleStringSchema()));
        env.execute();
    }
}
