package com.wildgoose.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.utils.DimUtil;
import com.wildgoose.gmall.realtime.utils.DruidDSUtil;
import com.wildgoose.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.SQLException;

/**
 * @author 张翔
 * @date 2023/2/26 11:05
 * @description
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private DruidDataSource druidDataSource = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {

//        //获取连接
//        DruidPooledConnection connection = druidDataSource.getConnection();
//
//        //写出数据
//        String sinkTable = jsonObj.getString("sinkTable");
//        JSONObject data = jsonObj.getJSONObject("data");
//        PhoenixUtil.upsertValues(connection,sinkTable,data);
//
//        //归还连接
//        connection.close();

        // 获取目标表表名
        String sinkTable = jsonObj.getString("sinkTable");
        // 获取操作类型
        String type = jsonObj.getString("type");
        // 获取 id 字段的值
        String id = jsonObj.getString("id");

        // 清除 JSON 对象中的 sinkTable 字段和 type 字段
        // 以便可将该对象直接用于 HBase 表的数据写入
        jsonObj.remove("sinkTable");
        jsonObj.remove("type");

        // 获取连接对象
        DruidPooledConnection conn = druidDataSource.getConnection();

        try {
            PhoenixUtil.upsertValues(conn, sinkTable, jsonObj);
        } catch (Exception e) {
            System.out.println("维度数据写入异常");
            e.printStackTrace();
        } finally {
            try {
                // 归还数据库连接对象
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库连接对象归还异常");
                sqlException.printStackTrace();
            }
        }

        // 如果操作类型为 update，则清除 redis 中的缓存信息
        if ("update".equals(type)) {
            DimUtil.deleteCached(sinkTable, id);
        }

    }
}
