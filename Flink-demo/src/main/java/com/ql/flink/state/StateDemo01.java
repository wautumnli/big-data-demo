package com.ql.flink.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wanqiuli
 */
public class StateDemo01 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启检查点机制，并指定状态检查点之间的时间间隔
        env.enableCheckpointing(1000);
        // 其他可选配置如下：
        // 设置语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置两个检查点之间的最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置执行Checkpoint操作时的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置最大并发执行的检查点的数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 将检查点持久化到外部存储
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 如果有更近的保存点时，是否将作业回退到该检查点
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);

        // 设置并行度为1
        DataStreamSource<Tuple2<String, Long>> tuple2DataStreamSource = env.setParallelism(1).fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 80L), Tuple2.of("a", 400L),
                Tuple2.of("a", 100L), Tuple2.of("a", 200L), Tuple2.of("a", 200L),
                Tuple2.of("b", 100L), Tuple2.of("b", 200L), Tuple2.of("b", 200L),
                Tuple2.of("b", 500L), Tuple2.of("b", 600L), Tuple2.of("b", 700L));
        tuple2DataStreamSource
                .flatMap(new ThresholdWarning01(100L, 3))
                .printToErr();
        env.execute("Managed Keyed State");
    }
}
