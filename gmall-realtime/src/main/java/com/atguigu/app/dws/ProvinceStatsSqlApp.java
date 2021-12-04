package com.atguigu.app.dws;

import com.atguigu.bean.ProvinceStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL的方式读取Kafka主题的数据  注意：提取事件时间生成WaterMark
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        tableEnv.executeSql("create table order_wide( " +
                "    province_id bigint, " +
                "    province_name String, " +
                "    province_area_code string, " +
                "    province_iso_code string, " +
                "    province_3166_2_code string, " +
                "    order_id bigint, " +
                "    split_total_amount DECIMAL, " +
                "    create_time string, " +
                "    rt as TO_TIMESTAMP(create_time), " +
                "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                ") WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic,groupId) + ")"
                );

        //TODO 3.计算订单数以及订单金额  分组 开窗 聚合
        Table tableResult = tableEnv.sqlQuery("select " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    sum(split_total_amount) order_amount, " +
                "    count(distinct order_id) order_count, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by province_id, " +
                "    province_name, " +
                "    province_area_code, " +
                "    province_iso_code, " +
                "    province_3166_2_code, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 4.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(tableResult, ProvinceStats.class);
        provinceStatsDS.print();
        //TODO 5.将流数据写出到CLickHouse
        provinceStatsDS.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_210625 values(?,?,?,?,?,?,?,?,?,?)"));

        //TODO 6.启动
        env.execute("ProvinceStatsSqlApp");
    }

}
