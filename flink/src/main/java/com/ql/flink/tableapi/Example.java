package com.ql.flink.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @date 2021-02-01 10:33
 * @author wanqiuli
 */
public class Example {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("D:\\wanqiuli\\Desktop\\big-data-demo\\flink\\src\\main\\resources\\example.txt");

        // 获取流
        SingleOutputStreamOperator<User> map = stringDataStreamSource.map(line -> {
            String[] users = line.split(" ");
            return new User(users[0], users[1], users[2]);
        });

        // 转成table api
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 指定source源名字
        tableEnv.createTemporaryView("test", map);
        // 执行sql
        Table table = tableEnv.sqlQuery("select id, name from test where id = '200'");
        // 结果转换
        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);
        result.print();

        env.execute();
    }
}
