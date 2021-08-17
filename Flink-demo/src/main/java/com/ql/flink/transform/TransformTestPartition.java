package com.ql.flink.transform;

import com.ql.flink.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wautumnli
 * @date: 2021-08-18 00:42
 **/
public class TransformTestPartition {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<String> inputStream = env.readTextFile("/Users/wautumnli/Desktop/mbp/work/big-data-demo/Flink-demo/src/main/resources/sensor.txt");
        inputStream.print("input");
        DataStream<String> shuffleStream = inputStream.shuffle();
        shuffleStream.print("shuffle");
        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });
        // keyBy按照hashcode重分区
        dataStream.keyBy("id").print("keyBy");
        // global送到下游到第一个分区
        dataStream.global().print("global");
        env.execute();
    }
}
