package com.wildgoose.gmall.realtime.common;

/**
 * @author 张翔
 * @date 2023/2/25 17:58
 * @description
 */
public class GmallConfig {
    // Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL_REALTIME";

    // Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    // Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:dev1,dev2,dev3:2181";

    // ClickHouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    // ClickHouse 连接 URL
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://dev2:8123/gmall";

    // ClickHouse 用户名
    public static final String CLICKHOUSE_USERNAME = "hadoop";

    // ClickHouse 密码
    public static final String CLICKHOUSE_PASSWORD = "123456";
}
