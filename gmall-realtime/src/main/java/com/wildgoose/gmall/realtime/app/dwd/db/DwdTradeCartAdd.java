package com.wildgoose.gmall.realtime.app.dwd.db;

import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.MysqlUtil;
import com.wildgoose.gmall.realtime.utils.PropertiesUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @author 张翔
 * @date 2023/2/28 17:09
 * @description
 */
public class DwdTradeCartAdd {

    public static void main(String[] args) throws Exception {

        // TODO 1.环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.1 设置 Table 中的时区为本地时区
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("GMT+8"));

        //1.2 获取配置对象
        Configuration configuration = tableEnv.getConfig().getConfiguration();

        //1.3 为表关联时状态中存储的数据设置过期事件
        configuration.setString("table.exec.state.ttl", "5s");

        // TODO 2.状态后端设置
//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        // TODO 3.从 Kafka 读取业务数据，封装为 Flink SQL 表
        tableEnv.executeSql("" +
                "create table topic_db(\n" +
                "`database` string,\n" +
                "`table` string,\n" +
                "`type` string,\n" +
                "`data` map<string, string>,\n" +
                "`old` map<string, string>,\n" +
                "`ts` string,\n" +
                "`proc_time` as PROCTIME()\n" +
                ")" + KafkaUtil.getKafkaDDL("topic_db", "dwd_trade_cart_add"));

        // TODO 4.读取购物车表数据
        Table cartAdd = tableEnv.sqlQuery("" +
                "select\n" +
                "data['id'] id,\n" +
                "data['user_id'] user_id,\n" +
                "data['sku_id'] sku_id,\n" +
                "data['source_id'] source_id,\n" +
                "data['source_type'] source_type,\n" +
                "if(`type` = 'insert',\n" +
                "data['sku_num'],cast((cast(data['sku_num'] as int) - cast(`old`['sku_num'] as int)) as string)) sku_num,\n" +
                "ts,\n" +
                "proc_time\n" +
                "from `topic_db` \n" +
                "where `table` = 'cart_info'\n" +
                "and (`type` = 'insert'\n" +
                "or (`type` = 'update' \n" +
                "and `old`['sku_num'] is not null \n" +
                "and cast(data['sku_num'] as int) > cast(`old`['sku_num'] as int)))"
        );

        tableEnv.createTemporaryView("cart_add", cartAdd);

        // TODO 5.建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 6.关联两张表获得加购明细表
        Table resultTable = tableEnv.sqlQuery("select\n" +
                "cadd.id,\n" +
                "user_id,\n" +
                "sku_id,\n" +
                "source_id,\n" +
                "source_type,\n" +
                "dic_name source_type_name,\n" +
                "sku_num,\n" +
                "ts\n" +
                "from cart_add cadd\n" +
                "join base_dic for system_time as of cadd.proc_time as dic\n" +
                "on cadd.source_type=dic.dic_code");

        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 7.建立 Kafka-Connector dwd_trade_cart_add 表
        tableEnv.executeSql("" +
                "create table dwd_trade_cart_add(\n" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "source_id string,\n" +
                "source_type_code string,\n" +
                "source_type_name string,\n" +
                "sku_num string,\n" +
                "ts string\n" +
                ")" + KafkaUtil.getKafkaSinkDDL("dwd_trade_cart_add"));

        // TODO 8.将关联结果写入
        tableEnv.executeSql("" +
                "insert into dwd_trade_cart_add select * from result_table"
        );

//        env.execute();
    }
}
