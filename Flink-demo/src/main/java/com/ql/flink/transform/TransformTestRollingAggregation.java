package com.ql.flink.transform;

import com.ql.flink.SensorReading;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wautumnli
 * @date: 2021-08-17 22:50
 **/
public class TransformTestRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/wautumnli/Desktop/mbp/work/big-data-demo/Flink-demo/src/main/resources/sensor.txt");

        // 转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        // KeyedStream<SensorReading, String> keyedStream1 = dataStream.keyBy(SensorReading::getId);

        // 取最大值
        DataStream<SensorReading> resultStream = keyedStream.maxBy("temperature");

        resultStream.print("result");

        env.execute();
    }
}
