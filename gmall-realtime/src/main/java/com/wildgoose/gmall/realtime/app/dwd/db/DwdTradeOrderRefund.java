package com.wildgoose.gmall.realtime.app.dwd.db;

import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.MysqlUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author 张翔
 * @date 2023/3/3 14:48
 * @description 交易域退单事务事实表
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 设置 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));
        //获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        //为表关联时状态中存储的数据设置过期事件
        configuration.setString("table.exec.state.ttl", "5s");

//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        // TODO 2. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`proc_time` as PROCTIME(),\n" +
                "`ts` string\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund"));

        // TODO 3. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['order_id'] order_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['refund_type'] refund_type,\n" +
                "data['refund_num'] refund_num,\n" +
                "data['refund_amount'] refund_amount,\n" +
                "data['refund_reason_type'] refund_reason_type,\n" +
                "data['refund_reason_txt'] refund_reason_txt,\n" +
                "data['create_time'] create_time,\n" +
                "proc_time,\n" +
                "ts\n" +
                "from topic_db\n" +
                "where `table` = 'order_refund_info'\n" +
                "and `type` = 'insert'\n");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 4. 读取订单表数据，筛选退单数据
        Table orderInfoRefund = tableEnv.sqlQuery("select\n" +
                "data['id'] id,\n" +
                "data['province_id'] province_id,\n" +
                "`old`\n" +
                "from topic_db\n" +
                "where `table` = 'order_info'\n" +
                "and `type` = 'update'\n" +
                "and data['order_status']='1005'\n" +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info_refund", orderInfoRefund);

        // TODO 5. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6. 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select \n" +
                "ri.id,\n" +
                "ri.user_id,\n" +
                "ri.order_id,\n" +
                "ri.sku_id,\n" +
                "oi.province_id,\n" +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id,\n" +
                "ri.create_time,\n" +
                "ri.refund_type,\n" +
                "type_dic.dic_name,\n" +
                "ri.refund_reason_type,\n" +
                "reason_dic.dic_name,\n" +
                "ri.refund_reason_txt,\n" +
                "ri.refund_num,\n" +
                "ri.refund_amount,\n" +
                "ri.ts,\n" +
                "current_row_timestamp() row_op_ts\n" +
                "from order_refund_info ri\n" +
                "join \n" +
                "order_info_refund oi\n" +
                "on ri.order_id = oi.id\n" +
                "join \n" +
                "base_dic for system_time as of ri.proc_time as type_dic\n" +
                "on ri.refund_type = type_dic.dic_code\n" +
                "join\n" +
                "base_dic for system_time as of ri.proc_time as reason_dic\n" +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7. 建立 Kafka-Connector dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "order_id string,\n" +
                "sku_id string,\n" +
                "province_id string,\n" +
                "date_id string,\n" +
                "create_time string,\n" +
                "refund_type_code string,\n" +
                "refund_type_name string,\n" +
                "refund_reason_type_code string,\n" +
                "refund_reason_type_name string,\n" +
                "refund_reason_txt string,\n" +
                "refund_num string,\n" +
                "refund_amount string,\n" +
                "ts string,\n" +
                "row_op_ts timestamp_ltz(3)\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_order_refund"));

        // TODO 8. 将关联结果写入 Kafka-Connector 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from result_table");

    }
}
