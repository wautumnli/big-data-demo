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
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: wautumnli
 * @date: 2021-01-29 09:25
 **/
public class CheckPointWarn extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Tuple2<String, Long>>>> implements CheckpointedFunction {

    private Long threshold;
    private transient ListState<Tuple2<String, Long>> checkPointState;
    private List<Tuple2<String, Long>> warnData;
    private Long count;


    public CheckPointWarn(Long threshold, Long count) {
        this.threshold = threshold;
        this.count = count;
        warnData = new ArrayList<>();
    }

    @Override
    public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, List<Tuple2<String, Long>>>> collector) throws Exception {
        if (stringLongTuple2.f1 > threshold) {
            warnData.add(stringLongTuple2);
        }

        if (warnData.size() >= count) {
            collector.collect(Tuple2.of(checkPointState.hashCode()+"", warnData));
            warnData.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        // 清除之前状态
        checkPointState.clear();

        // 记录新状态
        for (Tuple2<String, Long> warnDatum : warnData) {
            checkPointState.add(warnDatum);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        // 设置初始状态
        checkPointState = functionInitializationContext
                .getOperatorStateStore()
                .getListState(new ListStateDescriptor<Tuple2<String, Long>>
                        ("warnData", TypeInformation.of
                                (new TypeHint<Tuple2<String, Long>>() {})
                        )
                );

        // 如果重启 可以直接获取数据
        if (functionInitializationContext.isRestored()) {
            for (Tuple2<String, Long> stringLongTuple2 : checkPointState.get()) {
                warnData.add(stringLongTuple2);
            }
        }
    }
}
