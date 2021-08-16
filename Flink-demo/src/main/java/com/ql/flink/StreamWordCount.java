package com.ql.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wautumnli
 * @date: 2021-08-15 22:50
 **/
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        // env.setParallelism(7);

        // 从文件中获取数据
        // String inputPath = "/Users/wautumnli/Desktop/mbp/work/big-data-demo/Flink-demo/src/main/resources/hello.txt";
        // DataStream<String> inputDataStream = env.readTextFile(inputPath);
        // 从socket文本流中读取
        DataStream<String> inputDataStream = env.socketTextStream("localhost", 7777);

        // 基于数据流进行转换
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1)
                .setParallelism(2);

        resultStream.print()
                .setParallelism(1);

        // 执行任务
        env.execute();
    }
}
