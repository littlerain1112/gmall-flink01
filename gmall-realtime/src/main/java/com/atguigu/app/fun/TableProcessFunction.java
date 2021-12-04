package com.atguigu.app.fun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TableProcessFunction extends BroadcastProcessFunction <JSONObject,String,JSONObject>{

    //声明phoenix连接
    private Connection connection;

    //侧输出流标记属性
    private OutputTag<JSONObject> outputTag;

    //Map状态描述器属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    public TableProcessFunction (OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"db":"","tableName":"base_trademark","before":{},"after":{"id":"tm_name":"","logo_url":""}}
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

        //1.根据主键读取广播状态对应的数据
        String key = value.getString("tableName") + "-" + value.getString("type");
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        TableProcess tableProcess = broadcastState.get(key);

        //2.根据广播状态数据  过滤字段
        if (tableProcess != null) {
            filterColumns(value.getJSONObject("after"), tableProcess.getSinkColumns());

            //3.根据广播状态数据  分流
            //将表名或者主题名添加至数据中
            value.put("sinkTable",tableProcess.getSinkTable());
            value.put("pk",tableProcess.getSinkPk());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                collector.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                readOnlyContext.output(outputTag,value);
            }

        } else {
            System.out.println(key + "不存在");
        }


    }

    private void filterColumns(JSONObject after, String sinkColumns) {

        //切分字段
        String[] columns = sinkColumns.split(",");
        List<String> columnsList = Arrays.asList(columns);

        Set<Map.Entry<String, Object>> entries = after.entrySet();
        entries.removeIf(next -> !columnsList.contains(next.getKey()));
    }

    //value:{"db":"","tn":"table_process","before":{},"after":{"source_table":""...},"type":""}
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //1.解析数据为TableProcess对象

        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.校验HBase表是否存在，如果不存在则建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())){
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //3.将数据写入状态，广播出去

        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        String key = tableProcess.getSourceTable() + "-" + tableProcess.getOperateType();
        broadcastState.put(key,tableProcess);
    }

    //创建Phoenix表：create table if not exists db.tn(id varchar primary key,tm_name varchar,sex varchar) sinkExtend;
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend)  {

        if (sinkPk == null || sinkPk.equals("")) {
            sinkPk = "id";
        }

        if (sinkExtend == null){
            sinkExtend = "";
        }

        PreparedStatement preparedStatement = null;
        try{
            //构建建表语句
            StringBuilder createTableSql = new StringBuilder("create table if not exists  ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");

            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {

                //取出列名
                String column = columns[i];

                //判断当前字段是否为主键
                if (sinkPk.equals(column)) {
                    createTableSql.append(column).append(" varchar primary key ");
                } else {
                    createTableSql.append(column).append(" varchar ");
                }

                //判断是否为最后一个字段
                if (i < columns.length - 1) {
                    createTableSql.append(",");
                }
            }
                createTableSql.append(")").append(sinkExtend);

                //打印建表语句
                System.out.println(createTableSql);

                //执行
                preparedStatement = connection.prepareStatement(createTableSql.toString());
                preparedStatement.execute();

            } catch (SQLException e) {
                throw new RuntimeException("创建Phoenix表：" + sinkTable + "失败！");
            } finally {
                if (preparedStatement != null) {
                    try {
                        preparedStatement.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

