package com.wildgoose.gmall.realtime.app.dwd.db;

import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author 张翔
 * @date 2023/3/4 12:39
 * @description 用户域用户注册事务事实表
 */
public class DwdUserRegister {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
//        //获取配置对象
//        Configuration configuration = tableEnv.getConfig().getConfiguration();
//        //为表关联时状态中存储的数据设置过期事件
//        configuration.setString("table.exec.state.ttl", "5s");

//        // TODO 2.设置状体后端
//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        // TODO 3. 从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql(KafkaUtil.getTopicDb("dwd_trade_order_detail"));

        // TODO 4. 读取用户表数据
        Table userInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] user_id,\n" +
                "data['create_time'] create_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'user_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("user_info", userInfo);

        // TODO 5. 创建 Kafka-Connector dwd_user_register 表
        tableEnv.executeSql("create table `dwd_user_register`(\n" +
                "`user_id` string,\n" +
                "`date_id` string,\n" +
                "`create_time` string,\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_user_register"));

        // TODO 6. 将输入写入 Kafka-Connector 表
        tableEnv.executeSql("insert into dwd_user_register\n" +
                "select \n" +
                "user_id,\n" +
                "date_format(create_time, 'yyyy-MM-dd') date_id,\n" +
                "create_time,\n" +
                "ts\n" +
                "from user_info");
    }
}
