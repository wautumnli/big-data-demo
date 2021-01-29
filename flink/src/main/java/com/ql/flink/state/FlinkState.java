package com.ql.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: wautumnli
 * @date: 2021-01-28 20:53
 **/
public class FlinkState {

    public static void main(String[] args) throws Exception {
        test2();
    }

    public static void test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple2<String, Long>> source = env.fromElements(new Tuple2<>("a", 100L), new Tuple2<>("a", 200L), new Tuple2<>("a", 300L),
                new Tuple2<>("b", 500L), new Tuple2<>("b", 600L));
        source.keyBy(0)
                .flatMap(new ThresholdWarn(100L,2L))
                .printToErr();

        env.execute();
    }

    public static void test2() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置开启checkpoint
        env.enableCheckpointing(1000);
        // 数据比较少 把并行度设置的低一点 否则会达不到条件
        DataStreamSource<Tuple2<String, Long>> source = env
                .setParallelism(1)
                .fromElements(new Tuple2<>("a", 100L), new Tuple2<>("a", 200L), new Tuple2<>("a", 300L), new Tuple2<>("b", 500L), new Tuple2<>("b", 600L));
        source.flatMap(new CheckPointWarn(100L,2L))
                .printToErr();

        env.execute();
    }
}
