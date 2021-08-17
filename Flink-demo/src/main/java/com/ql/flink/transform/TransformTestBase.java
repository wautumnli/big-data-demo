package com.ql.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wautumnli
 * @date: 2021-08-17 22:37
 **/
public class TransformTestBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/wautumnli/Desktop/mbp/work/big-data-demo/Flink-demo/src/main/resources/sensor.txt");

        // map
        DataStream<Integer> mapStream = inputStream.map((MapFunction<String, Integer>) String::length);

        // flatMap
        DataStream<String> flatMapStream = inputStream.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] fields = value.split(",");
            for (String field : fields) {
                out.collect(field);
            }
        });

        // filter
        DataStream<String> filterStream = inputStream.filter((FilterFunction<String>) value -> value.startsWith("sensor_1"));

        mapStream.print("map");
        flatMapStream.print("flatMap");
        filterStream.print("filter");

        env.execute();
    }
}
