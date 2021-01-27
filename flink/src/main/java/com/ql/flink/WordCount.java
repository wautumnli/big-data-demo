package com.ql.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Author： wanqiuli
 * DateTime: 2021/1/27 9:36
 * Flink做单词统计,离线统计,批处理
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // 获取环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 文件路径
        String fliePath = "D:\\wanqiuli\\Desktop\\big-data-test\\flink\\src\\main\\resources\\hello.txt";

        // 获取文件集合
        // DataSet主要来做离线的数据集
        DataSet<String> dataSet = env.readTextFile(fliePath);

        DataSet<Tuple2<String, Integer>> data = dataSet.flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);

        data.print();

    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }


}
