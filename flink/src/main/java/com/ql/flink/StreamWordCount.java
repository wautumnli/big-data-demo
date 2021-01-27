package com.ql.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Author： wanqiuli
 * DateTime: 2021/1/27 10:04
 * 流处理单词统计
 */
public class StreamWordCount{

    public static void main(String[] args) throws Exception {
        // 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，因为处在单机，用线程模拟在多个机器上分布式计算
        //env.setParallelism(2);

        // 文件路径
        String fliePath = "D:\\wanqiuli\\Desktop\\big-data-test\\flink\\src\\main\\resources\\hello.txt";

        // 获取文件集合
        // DataSet主要来做离线的数据集
        DataStream<String> dataSet = env.readTextFile(fliePath);

        DataStream<Tuple2<String, Integer>> data = dataSet.flatMap(new WordCount.MyFlatMap())
                .keyBy(0)
                .sum(1);

        // 流计算通过execute源源不断的获取数据计算
        data.print();
        env.execute();

    }
}
