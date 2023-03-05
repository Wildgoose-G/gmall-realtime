package com.wildgoose.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.wildgoose.gmall.realtime.app.func.DimSinkFunction;
import com.wildgoose.gmall.realtime.app.func.TableProcessFunction;
import com.wildgoose.gmall.realtime.bean.TableProcess;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.LoggerUtil;
import com.wildgoose.gmall.realtime.utils.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author 张翔
 * @date 2023/2/23 15:53
 * @description
 */
public class DimSinkApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3); //生产环境设置为Kafka主题的分区数

//        //1.1 开启CheckPoint
//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        //1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        // TODO 2.读取Kafka topic_db主题数据创建主流
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getFlinkKafkaConsumer(PropertiesUtil.get("db.kafka.topic"), PropertiesUtil.get("db.kafka.groupId")));

        // TODO 3.过滤非JSON数据以及保留新增、变化、以及初始化的数据
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaDS.flatMap((String value, Collector<JSONObject> out) -> {
                    try {
                        JSONObject jsObj = JSON.parseObject(value);

                        //读取数据中的操作类型字段
                        String type = jsObj.getString("type");

                        //保留新增、变化以及初始化数据
                        if ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type)) {
                            out.collect(jsObj);
                        }
                    } catch (Exception e) {
                        LoggerUtil.log().error("发现脏数据：" + value);
                    }
                })
                .returns(JSONObject.class);

        // TODO 4.使用FlinkCDC读取MySQL配置信息表创建配置流
        MySqlSource<String> mysqlSource = MySqlSource
                .<String>builder()
                .hostname(PropertiesUtil.get("mysql.hostname"))
                .port(PropertiesUtil.getInt("mysql.port"))
                .username(PropertiesUtil.get("mysql.username"))
                .password(PropertiesUtil.get("mysql.password"))
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        // TODO 5.将配置流处理为广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlSourceDS.broadcast(mapStateDescriptor);

        // TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedDS = filterDS.connect(broadcastDS);

        // TODO 7.处理连接流，根据配置信息处理主流数据
        SingleOutputStreamOperator<JSONObject> dimDS = connectedDS.process(new TableProcessFunction(mapStateDescriptor));

        // TODO 8.将数据写出到Phoenix
        dimDS.addSink(new DimSinkFunction());

        env.execute();
    }
}
