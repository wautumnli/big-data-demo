package com.ql.flink.state;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author wanqiuli
 */
public class ThresholdWarning extends
        RichFlatMapFunction<Tuple2<String, Long>, Tuple2<String, List<Long>>> {

    /**
     * 通过ListState来存储非正常数据的状态
     */
    private transient ListState<Long> abnormalData;
    /**
     * 需要监控的阈值
     */
    private final Long threshold;
    /**
     * 触发报警的次数
     */
    private final Integer numberOfTimes;

    ThresholdWarning(Long threshold, Integer numberOfTimes) {
        this.threshold = threshold;
        this.numberOfTimes = numberOfTimes;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig
                // 设置有效期为 10 秒
                .newBuilder(Time.seconds(10))
                // 设置有效期更新规则，这里设置为当创建和写入时，都重置其有效期到规定的10秒
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 设置只要值过期就不可见，另外一个可选值是ReturnExpiredIfNotCleanedUp，
                // 代表即使值过期了，但如果还没有被物理删除，就是可见的
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>("abnormalData", Long.class);
        descriptor.enableTimeToLive(ttlConfig);

        // 通过状态名称(句柄)获取状态实例，如果不存在则会自动创建
        abnormalData = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<String, Long> value, Collector<Tuple2<String, List<Long>>> out)
            throws Exception {
        Long inputValue = value.f1;
        // 如果输入值超过阈值，则记录该次不正常的数据信息
        if (inputValue >= threshold) {
            abnormalData.add(inputValue);
        }
        ArrayList<Long> list = Lists.newArrayList(abnormalData.get().iterator());
        // 如果不正常的数据出现达到一定次数，则输出报警信息
        if (list.size() >= numberOfTimes) {
            out.collect(Tuple2.of(value.f0 + " 超过指定阈值 ", list));
            // 报警信息输出后，清空状态
            abnormalData.clear();
        }
    }
}
