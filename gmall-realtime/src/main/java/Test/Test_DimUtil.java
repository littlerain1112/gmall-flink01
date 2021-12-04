package Test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class Test_DimUtil {
    public static JSONObject getDimInfo(Connection connection,String tableName,String pk) throws Exception {
        //查询Redis
        Jedis jedis = Test_RedisUtil.getJedis();

        String redisKey = "DIM:" + tableName + ":" + pk;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {

            //重置过期时间
            jedis.expire(redisKey,3600*24);

            //归还连接
            jedis.close();

            //返回从Redis查询到的数据
            return JSON.parseObject(dimInfoStr);
        }

        //构建SQL语句 select * from db.tn where id = '1001'
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id = '" + pk + "'";

        //查询phoenix
        List<JSONObject> queryList = Test_JdbcUtil.queryList(connection, querySql, JSONObject.class, false);

        JSONObject dimInfo = queryList.get(0);

        //将数据写入Redis
        jedis.set(redisKey,dimInfo.toJSONString());
        jedis.expire(redisKey,3600*24);

        jedis.close();

        //返回结果
        return dimInfo;

    }

    public static void delDimInfo(String tableName,String pk){
        String redisKey = "DIM:" + tableName +":" + pk;
        Jedis jedis = Test_RedisUtil.getJedis();
        jedis.del(redisKey);

        jedis.close();
    }

    public static void main(String[] args) throws Exception{
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_BASE_CATEGORY1","19"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_CATEGORY1", "19"));
        long end2 = System.currentTimeMillis();

        System.out.println(end - start);
        System.out.println(end2 - end);

        connection.close();
    }
}
