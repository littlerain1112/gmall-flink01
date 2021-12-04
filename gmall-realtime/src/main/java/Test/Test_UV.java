package Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class Test_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource("dwd_page_log", "page_log_0625"));

        SingleOutputStreamOperator<JSONObject> jsonObj = streamSource.map(JSON::parseObject);

        KeyedStream<JSONObject, String> keyedStream = jsonObj.keyBy(j -> j.getJSONObject("common").getString("mid"));

        SingleOutputStreamOperator<JSONObject> filterDs = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("dt-state", String.class);
                StateTtlConfig stateTtlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                valueStateDescriptor.enableTimeToLive(stateTtlConfig);
                valueState = getRuntimeContext().getState(valueStateDescriptor);

                sdf = new SimpleDateFormat("yyyy-MM-dd");
            }

            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                if (lastPageId == null || lastPageId.equals("")) {

                    String dt = valueState.value();

                    Long ts = jsonObject.getLong("ts");
                    String curDt = sdf.format(ts);

                    if (dt == null || !dt.equals(curDt)) {
                        valueState.update(curDt);
                        return true;
                    }
                }
                return false;
            }
        });

        filterDs.print();
        filterDs.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));

        env.execute("UV");
    }
}
