package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp01 {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.消费Kafka ods_base_log 主题数据
        DataStreamSource<String> kafkaSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210625"));

        //TODO 3.过滤脏数据  使用侧输出流

        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};

        //侧输出流 只能用process  ，过滤脏数据需要转为JSON格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    //将转换成功的数据 写入主流
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    //转换失败，将数据写入侧输出流
                    context.output(dirtyTag, value);
                }

            }
        });

        //提取侧输出流中的脏数据
        DataStream<String> sideOutput = jsonObjDS.getSideOutput(dirtyTag);
        sideOutput.print("Dirty>>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //TODO 5.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            //定义一个状态 做初始化
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                //获取新老用户标记
                String isNew = jsonObject.getJSONObject("common").getString("is_new");

                //判断是否为“1”
                if ("1".equals(isNew)) {

                    String state = valueState.value();

                    if (state != null) {
                        jsonObject.getJSONObject("common").put("is_new", "0");
                    } else {
                        valueState.update("0");
                    }

                }
                return jsonObject;

            }
        });


        //TODO 6.分流  使用侧输出流   页面->主流   启动和曝光——>侧输出流
        //使用process
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //获取启动数据
                String start = value.getString("start");
                if (start != null) {
                    //将数据写入启动日志侧输出流
                    context.output(startTag, value.toJSONString());
                } else {
                    //非启动日志，页面日志，将数据写入主流
                    collector.collect(value.toJSONString());

                    //获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");

                    if (displays != null && displays.size() > 0) {

                        //提取页面ID
                        String pageId = value.getJSONObject("page").getString("page_id");

                        //遍历写出
                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);
                            display.put("page_id", pageId);

                            //将数据写入曝光侧输出流
                            context.output(displayTag, display.toJSONString());

                        }
                    }
                }

            }
        });

        //TODO 7.将3个流写入对应的Kafka主题
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page>>>>>>>>>>>>");
        startDS.print(("Start>>>>>>>>>"));
        displayDS.print("Display>>>>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp01");

    }
}
