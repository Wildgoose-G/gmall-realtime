package com.wildgoose.gmall.realtime.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.utils.DimUtil;
import com.wildgoose.gmall.realtime.utils.DruidDSUtil;
import com.wildgoose.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * @author 张翔
 * @date 2023/3/8 16:54
 * @description
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    // 表名
    private String tableName;

    // 线程池操作对象
    private ExecutorService executorService;

    // 德鲁伊连接池
    private DruidDataSource druidDataSource;

    public DimAsyncFunction(String tableName) {
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
        // 创建连接池
        druidDataSource = DruidDSUtil.createDataSource();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //从线程池中获取线程，发送异步请求
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            // 1. 根据流中的对象获取维度的主键
                            String key = getKey(obj);

                            // 2. 根据维度的主键获取维度对象
                            // 获取数据库连接对象
                            DruidPooledConnection conn = druidDataSource.getConnection();

                            JSONObject dimJsonObj = null;
                            try {
                                dimJsonObj = DimUtil.getDimInfo(conn, tableName, key);
                            } catch (Exception e) {
                                System.out.println("维度数据异步查询异常");
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

                            // 3. 将查询出来的维度信息 补充到流中的对象属性上
                            if (dimJsonObj != null) {
                                join(obj, dimJsonObj);
                            }
                            resultFuture.complete(Collections.singleton(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException("异步维度关联发生异常了");
                        }
                    }
                }
        );
    }
}
