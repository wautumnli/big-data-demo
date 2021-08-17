package com.ql.flink.source;

import com.ql.flink.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @author: wautumnli
 * @date: 2021-08-16 23:58
 **/
public class SourceTestUDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件中获取
        DataStream<SensorReading> dataStream = env.addSource(new MySensorSource());

        dataStream.print();

        env.execute();
    }

    // 实现自定义sourceFunction
    public static class MySensorSource implements SourceFunction<SensorReading> {

        // 来一个标记控制数据产生
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            Random random = new Random();
            HashMap<String, Double> sensorTempMap = new HashMap<>();
            // 产生一个温度集合
            for (int i = 0; i < 10; i++) {
                sensorTempMap.put("sensor_" + (i + 1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                // 更新温度集合
                for (String sensorId : sensorTempMap.keySet()) {
                    Double newTemp = sensorTempMap.get(sensorId) + random.nextGaussian();
                    sensorTempMap.put(sensorId, newTemp);
                    ctx.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));
                }
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
