package Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.fun.MyFlinkCDCDeSer;
import com.atguigu.app.fun.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class Test01_Db {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource("", ""));

        SingleOutputStreamOperator<JSONObject> jsonObjectSingleOutputStreamOperator = streamSource.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjectSingleOutputStreamOperator.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                return !"delete".equals(jsonObject.getString("type"));
            }
        });

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("flink-realtime")
                .tableList("")
                .startupOptions(StartupOptions.initial())

                .build();

        DataStreamSource<String> cdcDS = env.addSource(sourceFunction);

        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);

        BroadcastStream<String> broadcastStream = cdcDS.broadcast(stateDescriptor);

        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("hbaseTage") {
        };

        SingleOutputStreamOperator<JSONObject> kafkaMainDS = connectedStream.process(new TableProcessFunction(outputTag, stateDescriptor));

        DataStream<JSONObject> sideOutput = kafkaMainDS.getSideOutput(outputTag);

        sideOutput.print("Hbase:");

        kafkaMainDS.print("kafka:");

        sideOutput.addSink(new Test_DimSinkFunc());
        kafkaMainDS.addSink(MyKafkaUtil.getKafkaSink((KafkaSerializationSchema<JSONObject>) (jsonObject, aLong) -> new ProducerRecord<>(jsonObject.getString("sinkTable"),jsonObject.getString("after").getBytes())));


        //启动任务
        env.execute();



    }
}
