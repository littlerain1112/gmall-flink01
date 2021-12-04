package com.atguigu.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection, String querySql, Class<T> clz, boolean underScoreToCamel) throws Exception {

        //创建集合用于存放查询结果
        ArrayList<T> result = new ArrayList<>();

        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //获取列名信息
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        //遍历resultSet，将每行查询到的数据封装为 T 对象
        while (resultSet.next()) {

            //构建T对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount + 1 ; i++) {

                String columnName = metaData.getColumnName(i);
                Object value = resultSet.getObject(columnName);

                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //给T对象进行属性赋值
                BeanUtils.setProperty(t,columnName,value);

            }

            //将T对象添加至集合
            result.add(t);
        }

        //关闭资源对象
        resultSet.close();
        preparedStatement.close();

        //返回结果
        return result;
    }

    public static void main(String[] args) throws Exception {

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> jsonObjects = queryList(connection, "select * from GMALL_REALTIME.DIM_BASE_CATEGORY1", JSONObject.class, false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

    }
}