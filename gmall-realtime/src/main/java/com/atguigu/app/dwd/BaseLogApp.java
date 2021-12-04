package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


//数据流：web/app -> Nginx -> 日志服务器 -> Kafka（ODS）-> FlinkAPP -> kafka(DWD)
//程  序：Mock  -> Nginx  ->  Logger.sh  -> kafka(ZK) -> BaseLogApp ->kafka

public class BaseLogApp {
    public static void main(String[] args) throws Exception {

        //TODO 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境设置跟Kafka的分区数保持一致

       /* //设置状态后端
        env.setStateBackend(new FsStateBackend("")); //默认memory
        //开启CK
        env.enableCheckpointing(5000); //生产环境设置分钟级
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);*/

        //TODO 消费Kafka ods_base_log  主题数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210625"));

        //TODO 过滤脏数据  使用侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("Dirty") {
        };

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //将转换成功的 数据写入主流
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //转换失败，将数据写入侧输出流
                    context.output(outputTag, value);
                }

            }
        });

        //提取侧输出流中的脏数据
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(outputTag);
        sideOutput.print("Dirty>>>>>>>>");

        //TODO 按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO 新老用户校验 状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjectWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取新老用户标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断是否为1；
                if ("1".equals(isNew)) {

                    //获取状态数据，并判断状态是否为null
                    String state = valueState.value();

                    if (state == null) {
                        //更新状态
                        valueState.update("1");
                    } else {
                        //更新数据
                        value.getJSONObject("common").put("is_new", "0");
                    }

                }

                //返回结果
                return value;
            }
        });

//  jsonObjectWithNewFlagDS.print("JSON>>>>>>>>>");

        //TODO 分流 将页面日志 主流  启动和曝光 侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = jsonObjectWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {

                //获取启动数据
                String start = value.getString("start");
                if (start != null) {
                    //将数据写入启动日志侧输出流
                    context.output(startTag, value);

                } else {

                    //将数据写入页面日志主流
                    out.collect(value);

                    //获取曝光数据字段
                    JSONArray displays = value.getJSONArray("displays");

                    //判断是否存在曝光数据
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {

                        //遍历曝光数据，写出数据到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //补充页面id字段和时间戳
                            display.put("ts", ts);
                            display.put("page_id", pageId);

                            //将数据到曝光侧输出流
                            context.output(displayTag, display);
                        }
                    }

                }


            }
        });


        //TODO 提取各个流的数据
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page>>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");

        //TODO 将 数据写入Kafka主题
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 启动任务
        env.execute("BaseLogApp");

    }
}
