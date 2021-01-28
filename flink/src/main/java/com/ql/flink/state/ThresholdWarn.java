package com.ql.flink.state;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author: wautumnli
 * @date: 2021-01-28 21:01
 * 当超过阈值几次后 会警告
 **/
public class ThresholdWarn extends RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    // 阈值
    private Long threshold;
    // 警告次数
    private Long count;
    // 状态存储
    private ListState<Long> warnData;

    public ThresholdWarn(Long threshold, Long count) {
        this.threshold = threshold;
        this.count = count;
    }


    @Override
    public void flatMap(Tuple2<String, Long> stringLongTuple2, Collector<Tuple2<String, List<Long>>> collector) throws Exception {
        Long value = stringLongTuple2.f1;
        if (value > threshold) {
            warnData.add(value);
        }

        List<Long> list = Lists.newArrayList(warnData.get().iterator());
        if (list.size() >=  count) {
            collector.collect(Tuple2.of(stringLongTuple2.f0 + "警告", list));
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 获取warnData运行时状态
        warnData = getRuntimeContext().getListState(new ListStateDescriptor<Long>("warnData", Long.class));
    }
}
