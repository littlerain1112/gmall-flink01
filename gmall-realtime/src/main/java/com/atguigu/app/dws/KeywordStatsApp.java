package com.atguigu.app.dws;

import com.atguigu.app.fun.SplitFunction;
import com.atguigu.bean.KeywordStats;
import com.atguigu.utils.ClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取Kafka主题数据创建动态表  注意：提取事件时间生成WaterMark
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";

        tableEnv.executeSql(""  +
                "create table page_view( " +
                        "    common MAP<STRING,STRING>, " +
                        "    page MAP<STRING,STRING>, " +
                        "    ts bigint, " +
                        "    rt as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)), " +
                        "    WATERMARK FOR rt AS rt - INTERVAL '2' SECOND " +
                        ") with (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")"
                );

        //TODO 3.过滤数据
        Table fullWordTable = tableEnv.sqlQuery("" +
                "select " +
                "    page['item'] fullWord, " +
                "    rt " +
                "from page_view " +
                "where " +
                "    page['item_type']='keyword' and page['item'] is not null");

        //TODO 4.注册UDTF函数，炸裂搜索的关键词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        tableEnv.createTemporaryView("full_word",fullWordTable);
        Table keyWordTable = tableEnv.sqlQuery("" +
                "SELECT  " +
                "    word, " +
                "    rt " +
                "FROM full_word, LATERAL TABLE(SplitFunction(fullWord))");

        //TODO 5.按照单词分组，计算WordCount
        tableEnv.createTemporaryView("word_table",keyWordTable);
        Table tableResult = tableEnv.sqlQuery("" +
                "select " +
                "    'search' source, " +
                "    DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt, " +
                "    DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt, " +
                "    word keyword, " +
                "    count(*) ct, " +
                "    UNIX_TIMESTAMP()*1000 ts " +
                "from word_table " +
                "group by " +
                "    word, " +
                "    TUMBLE(rt, INTERVAL '10' SECOND)");

        //TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(tableResult, KeywordStats.class);

        //TODO 7.将数据写出到ClickHouse
        keywordStatsDS.print();
        keywordStatsDS.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats_210625(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        //TODO 8.启动
        env.execute("KeywordStatsApp");

    }

}
