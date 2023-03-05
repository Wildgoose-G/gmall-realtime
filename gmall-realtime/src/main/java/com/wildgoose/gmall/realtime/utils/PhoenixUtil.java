package com.wildgoose.gmall.realtime.utils;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author 张翔
 * @date 2023/2/26 11:13
 * @description
 */
public class PhoenixUtil {

    /**
     * @param connection Phoneix连接
     * @param sinkTable  表名
     * @param data       数据
     */
    public static void upsertValues(DruidPooledConnection connection, String sinkTable, JSONObject data) throws SQLException {

        //1. 拼接SQL语句 upsert into dn.tn(id, name, sex) values('', '', '')
        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();
        String sql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("
                + StringUtils.join(columns, ",")
                + ") values ('"
                + StringUtils.join(values, "','")
                + "')";
        ;

        //2. 预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        //3. 执行
        preparedStatement.execute();
        connection.commit();

        //4. 释放资源
        preparedStatement.close();
    }
}
