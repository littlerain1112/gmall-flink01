package com.atguigu.app.fun;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    //声明phoenix连接
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        //连接初始化
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value:{"db":"","tableName":"","before":{},"after":{"id":"...},"type":""}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;

        try {
            //拼接插入数据的SQL语句 upsert into db.tn(id,name,sex) values('1001','zhangsan','male')
            String upsertSql = genUpsertSQL(value.getString("sinkTable"), value.getJSONObject("after"));

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //如果维度数据更新，则先删除Redis中的数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delDimInfo(value.getString("sinkTable").toUpperCase(),
                        value.getJSONObject("after").getString("id"));
            }

            //执行写入数据操作
            preparedStatement.execute();

            //提交
            connection.commit();
        } catch (SQLException e) {

            e.printStackTrace();
            System.out.println("插入数据失败");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    private String genUpsertSQL(String sinkTable, JSONObject after) {

        //获取列名和数据
        Set<String> columns = after.keySet();
        Collection<Object> values = after.values();

        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
    }
}
