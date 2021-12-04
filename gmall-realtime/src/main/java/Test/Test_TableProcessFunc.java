package Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Test_TableProcessFunc extends BroadcastProcessFunction <JSONObject,String,JSONObject> {

    //声明phoenix连接
    private Connection connection;

    //侧输出流标记属性
    private OutputTag<JSONObject> outputTag;

    //Map状态描述器属性
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;

    // 构造器
    public Test_TableProcessFunc(OutputTag<JSONObject> outputTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.outputTag = outputTag;
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //加载驱动
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }


    //广播流数据处理方法：1.获取并解析数据，方便主流操作；2.校验表是否存在，如果不存在则需要在phoenix中建表 checkTable；3.写入状态，广播出去
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

        //解析数据为TableProcess对象
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        //2.建表
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType())) {
            checkTable(
                    tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend()
            );
        }

    }

    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {

        if (sinkPk == null){
            sinkPk = "id";
        }

        if (sinkExtend == null) {
            sinkExtend = "";
        }

        PreparedStatement preparedStatement = null;

        //构建建表语句
        StringBuilder createTableSQL = new StringBuilder("create table if not exists ")
                .append(GmallConfig.HBASE_SCHEMA)
                .append(".")
                .append(sinkTable)
                .append("(");

        String[] columns = sinkColumns.split(",");



    }


    //主流数据处理方法：1.获取广播的配置数据 2.过滤字段 filterColumn 3.根据输出类型（Kafka/HBase）分流
    @Override
    public void processElement(JSONObject jsonObject, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {

    }






}
