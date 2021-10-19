package com.ql.flink.state;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author wanqiuli
 */
public class ListStateDemo extends RichFlatMapFunction<Tuple2<String, Long>, List<Tuple2<String, Long>>> {

    private transient ListState<Tuple2<String, Long>> listState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<Tuple2<String, Long>> listStateDescriptor = new ListStateDescriptor(
                "listState",
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })
        );
        listState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<List<Tuple2<String, Long>>> out) throws Exception {
        List<Tuple2<String, Long>> currentListState = Lists.newArrayList(listState.get().iterator());
        currentListState.add(value);
        listState.update(currentListState);
        out.collect(currentListState);
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> dataStream = env.fromElements(
                Tuple2.of("a", 50L), Tuple2.of("a", 60L), Tuple2.of("a", 70L),
                Tuple2.of("b", 50L), Tuple2.of("b", 60L), Tuple2.of("b", 70L),
                Tuple2.of("c", 50L), Tuple2.of("c", 60L), Tuple2.of("c", 70L)
        );
        dataStream
                .keyBy(0)
                .flatMap(new ListStateDemo())
                .print();
        env.execute(ListStateDemo.class.getSimpleName());
    }
}
