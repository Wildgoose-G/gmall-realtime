package com.wildgoose.gmall.realtime.utils;

import com.wildgoose.gmall.realtime.bean.TransientSink;
import com.wildgoose.gmall.realtime.common.GmallConfig;
import lombok.SneakyThrows;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author 张翔
 * @date 2023/3/5 11:09
 * @description Clickhouse 工具类
 */
public class ClickHouseUtil {

    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @SneakyThrows
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {

                        // 使用反射的方式获取 T 对象中的数据
                        Field[] declaredFields = obj.getClass().getDeclaredFields();

                        //遍历属性
                        int offset = 0;
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            field.setAccessible(true); // 禁用安全检查

                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if (transientSink != null) {
                                offset++;
                                continue;
                            }

                            //获取属性值
                            Object value = field.get(obj);

                            //给占位符赋值
                            preparedStatement.setObject(i + 1 - offset, value);
                        }

                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(5000L)
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withUsername(GmallConfig.CLICKHOUSE_USERNAME)
                        .withPassword(GmallConfig.CLICKHOUSE_PASSWORD)
                        .build()
        );
    }

}
