package com.atguigu.utils;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {

    public static <T> SinkFunction<T> getClickHouseSink(String sql) {

        return JdbcSink.<T>sink(sql,  //需要执行的SQL语句
                //执行写入操作  就是将当前流中的对象属性赋值给SQL的占位符 insert into visitor_stats_210625 values(?,?,?)
                new JdbcStatementBuilder<T>() {
            //t 就算流中的一条数据对象
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        try {

                            //通过反射的方式获取当前JavaBean的所有字段
                            //反射应用，程序运行过程中去获取类的属性值、变量值等;
                            Class<?> clz = t.getClass();
                            Field[] fields = clz.getDeclaredFields();

                            //获取方法名称
//                            Method[] methods = clz.getMethods();
//                            for (int i = 0; i < methods.length; i++) {
//                                Method method = methods[i];
//                                Object invoke = method.invoke(t);
//                            }

                            //遍历字段数组

                            //跳出的属性计数
                            int offset = 0;

                            for (int i = 0; i < fields.length; i++) {

                                //获取字段
                                Field field = fields[i];

                                //设置该字段的访问权限
                                field.setAccessible(true);

                                //通过属性对象获取属性
                                TransientSink transientSink = field.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    offset++;
                                    continue;
                                }

                                //获取该字段对应的值信息
                                Object value = field.get(t);

                                //给预编译SQL占位符赋值
                                preparedStatement.setObject(i + 1 - offset, value);
                            }
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

}
