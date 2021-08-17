package com.ql.flink.transform;

import com.ql.flink.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * @author: wautumnli
 * @date: 2021-08-17 23:23
 **/
public class TransformTestMultipleStreams {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> inputStream = env.readTextFile("/Users/wautumnli/Desktop/mbp/work/big-data-demo/Flink-demo/src/main/resources/sensor.txt");

        // 转换成sensorReading
        DataStream<SensorReading> dataStream = inputStream.map(value -> {
            String[] fields = value.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 分流
        SplitStream<SensorReading> splitStream = dataStream.split(value -> (value.getTemperature() > 30 ? Collections.singletonList("high") : Collections.singletonList("low")));

        DataStream<SensorReading> highTempStream = splitStream.select("high");
        DataStream<SensorReading> lowTempStream = splitStream.select("low");
        DataStream<SensorReading> allTempStream = splitStream.select("high", "low");

        highTempStream.print("high");
        lowTempStream.print("low");
        allTempStream.print("all");

        // connect, high转二元组然后与low合并输出
        DataStream<Tuple2<String, Double>> warningStream = highTempStream
                .map((MapFunction<SensorReading, Tuple2<String, Double>>) value -> new Tuple2<>(value.getId(), value.getTemperature()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE)); // flink对lambda未知类型不支持，需要设置返回的类型
        // 合并
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectStream = warningStream.connect(lowTempStream);
        DataStream<Object> resultStream = connectStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) {
                return new Tuple3<>(value.f0, value.f1, "高温报警");
            }

            @Override
            public Object map2(SensorReading value) {
                return new Tuple2<>(value.getId(), "normal");
            }
        });
        resultStream.print("result");

        // union合并
        // DataStream<SensorReading> union = highTempStream.union(lowTempStream, allTempStream);

        env.execute();
    }
}
