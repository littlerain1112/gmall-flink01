package Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
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

public class Test_BaseLogApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_logApp_0625"));

        OutputTag<String> dirtyTag = new OutputTag<String>("dirty") {
        };

        SingleOutputStreamOperator<JSONObject> JSONobj = streamSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {

                    context.output(dirtyTag, s);
                }
            }
        });

        JSONobj.getSideOutput(dirtyTag).print("Dirty:");

        KeyedStream<JSONObject, String> keyedStream = JSONobj.keyBy(j -> j.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> newFlag = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));

            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //?????????????????????
                String isNew = value.getJSONObject("common").getString("is_new");

                if ("1".equals(isNew)) {
                    String state = valueState.value();

                    if (state == null) {
                        valueState.update("1");
                    } else {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }

                return value;
            }
        });

        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start"){};
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = newFlag.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                //??????????????????
                String start = jsonObject.getString("start");
                if (start != null) {
                    context.output(startTag, jsonObject);
                } else {
                    collector.collect(jsonObject);

                    //????????????????????????
                    JSONArray displays = jsonObject.getJSONArray("displays");

                    //??????????????????????????????
                    Long ts = jsonObject.getLong("ts");
                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {

                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            //????????????id?????? page_id ??? ?????????ts
                            display.put("ts", ts);
                            display.put("page_id", pageId);

                            //?????????????????????????????????
                            context.output(displayTag, display);
                        }
                    }
                }
            }
        });

        //????????????????????????
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print();
        startDS.print();
        displayDS.print();

        //???????????????Kafka??????
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        env.execute();

    }
}
