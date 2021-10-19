package com.ql.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wanqiuli
 */
public class OperateStateDemo extends RichFlatMapFunction<Tuple2<String, Long>, List<Tuple2<String, Long>>>
        implements CheckpointedFunction {

    private final int threshold;
    private transient ListState<Tuple2<String, Long>> checkpointedState;
    private final List<Tuple2<String, Long>> bufferedElements;

    public OperateStateDemo(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<List<Tuple2<String, Long>>> out) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            out.collect(bufferedElements);
            bufferedElements.clear();
        }
    }

    /**
     * 进行checkpoint快照
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Long> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Long>> listStateDescriptor = new ListStateDescriptor(
                "listState",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })
        );
        checkpointedState = context.getOperatorStateStore().getListState(listStateDescriptor);
        // 如果是故障恢复
        if (context.isRestored()) {
            for (Tuple2<String, Long> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
            checkpointedState.clear();
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(500);
        env.setParallelism(1);
        DataStream<Tuple2<String, Long>> dataStream = env.fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 60L), Tuple2.of("a", 70L),
                Tuple2.of("b", 50L), Tuple2.of("b", 60L), Tuple2.of("b", 70L),
                Tuple2.of("c", 50L), Tuple2.of("c", 60L), Tuple2.of("c", 70L)
        );
        dataStream
                .flatMap(new OperateStateDemo(2))
                .print();
        env.execute(OperateStateDemo.class.getSimpleName());
    }
}
