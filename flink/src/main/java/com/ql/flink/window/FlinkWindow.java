package com.ql.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: wautumnli
 * @date: 2021-01-28 20:10
 **/
public class FlinkWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //  从nc中获取数据 每隔5秒汇总一次
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9999, "\n", 3);
        stream.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (sentence, collector) -> {
            String[] words = sentence.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print();

        env.execute();
    }
}
