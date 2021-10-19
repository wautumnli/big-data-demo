package com.ql.flink.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author wanqiuli
 */
public class WindowDemo01 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 接收socket上的数据输入
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop001", 9999, "\n", 3);
        streamSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
                    String[] words = value.split("\t");
                    for (String word : words) {
                        out.collect(new Tuple2<>(word, 1L));
                    }
                }).keyBy(0)
                // 每隔3秒统计一次每个单词出现的数量
                // .timeWindow(Time.seconds(3))
                // 每隔3秒统计一次过去1分钟内的数据
                .timeWindow(Time.minutes(1), Time.seconds(3))
                .sum(1)
                .print();
        env.execute("Flink Streaming");
    }
}
