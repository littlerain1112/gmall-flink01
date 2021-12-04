package Test;

import com.atguigu.bean.TransientSink;
import com.atguigu.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Test_ClickHouseUtil {

    public static <T>SinkFunction<T> getClickHouseSink(String sql) {

        return JdbcSink.<T>sink(sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                        //通过反射的方式获取当前JavaBean的所有字段
                        Class<?> clz = t.getClass();
                        Field[] fields = clz.getDeclaredFields();

                        //遍历字段数组
                        int offset = 0;
                        for (int i = 0; i < fields.length; i++) {

                            //获取字段
                            Field field = fields[i];

                            //设置该字段的访问权限
                            field.setAccessible(true);

                            //获取字段上的注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null){
                                offset++;
                                continue;
                            }

                            //获取该字段对应的值信息
                            Object value = field.get(t);

                            //给预编译SQL占位符赋值
                            preparedStatement.setObject(i + 1 -offset,value);

                        }

                    }
                },new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }

}
