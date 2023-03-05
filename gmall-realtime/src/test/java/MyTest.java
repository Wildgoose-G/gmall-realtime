import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.wildgoose.gmall.realtime.bean.KeywordBean;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.KeywordUtil;
import com.wildgoose.gmall.realtime.utils.LoggerUtil;
import com.wildgoose.gmall.realtime.utils.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 张翔
 * @date 2023/2/23 17:37
 * @description
 */
public class MyTest {
    @org.junit.Test
    public void propTest() {
        System.out.println(PropertiesUtil.get("test"));
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        MySqlSource<String> mysqlSource = MySqlSource
                .<String>builder()
                .hostname(PropertiesUtil.get("mysql.hostname"))
                .port(PropertiesUtil.getInt("mysql.port"))
                .username(PropertiesUtil.get("mysql.username"))
                .password(PropertiesUtil.get("mysql.password"))
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> mysqlSourceDS = env.fromSource(mysqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");

        mysqlSourceDS.print();

        env.execute();
    }

    @org.junit.Test
    public void test1() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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

        filterDS.print();

        env.execute();
    }


    @org.junit.Test
    public void sqlTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 5 * 1000L),
                        new Event("Cary", "./home", 60 * 1000L),
                        new Event("Bob", "./prod?id=3", 90 * 1000L),
                        new Event("Alice", "./prod?id=7", 105 * 1000L)
                );

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表
        tableEnv.createTemporaryView("EventTable", eventStream);

        // 查询 Alice 的访问 url 列表
        Table aliceVisitTable = tableEnv.sqlQuery("SELECT url, user FROM EventTable WHERE user = 'Alice'");

        // 统计每个用户的点击次数
        Table urlCountTable = tableEnv.sqlQuery("SELECT user, COUNT(url) FROM EventTable GROUP BY user");

        // 将表转换成数据流，在控制台打印输出
        tableEnv.toDataStream(aliceVisitTable).print("alice visit");
        tableEnv.toChangelogStream(urlCountTable).print("count");
//        aliceVisitTable.execute().print();
//        urlCountTable.execute().print();

//        env.execute();
    }

    @org.junit.Test
    public void upsertConnectorTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socket1Stream = env.socketTextStream("localhost", 8888);
        DataStreamSource<String> socket2Stream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<TestBean> stream1 = socket1Stream
                .map(new MapFunction<String, TestBean>() {
                    @Override
                    public TestBean map(String s) throws Exception {
                        String[] eles = s.split(",");
                        TestBean testBean = new TestBean(
                                eles[0],
                                eles[1],
                                eles[2]
                        );
                        return testBean;
                    }
                });

        SingleOutputStreamOperator<TestBean> stream2 = socket2Stream
                .map(new MapFunction<String, TestBean>() {
                    @Override
                    public TestBean map(String s) throws Exception {
                        String[] eles = s.split(",");
                        TestBean testBean = new TestBean(
                                eles[0],
                                eles[1],
                                eles[2]
                        );
                        return testBean;
                    }
                });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        Table table1 = tableEnv.fromDataStream(stream1);
        Table table2 = tableEnv.fromDataStream(stream2);

        tableEnv.createTemporaryView("table1", table1);
        tableEnv.createTemporaryView("table2", table2);

        Table joinTable = tableEnv.sqlQuery("select table1.id,table1.name,table2.addr from table1 left join table2 on table1.id = table2.id");
        tableEnv.createTemporaryView("joinTable", joinTable);

//        tableEnv.executeSql(
//                "CREATE TABLE kafkaSink (\n" +
//                        "  `id` STRING,\n" +
//                        "  `name` STRING,\n" +
//                        "  `addr` STRING\n" +
//                        ") WITH (\n" +
//                        "  'connector' = 'kafka',\n" +
//                        "  'topic' = 'test',\n" +
//                        "  'properties.bootstrap.servers' = 'dev1:9092',\n" +
//                        "  'properties.group.id' = 'testGroup',\n" +
//                        "  'format' = 'json'\n" +
//                        ")"
//        );

        tableEnv.executeSql(
                "CREATE TABLE upsertKafkaSink (\n" +
                        "  `id` STRING,\n" +
                        "  `name` STRING,\n" +
                        "  `addr` STRING,\n" +
                        "PRIMARY KEY (id) NOT ENFORCED \n" +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'test',\n" +
                        "  'properties.bootstrap.servers' = 'dev1:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")"
        );

        tableEnv.executeSql("insert into kafkaSink select id,name,addr from joinTable");

        env.execute();

    }

    @org.junit.Test
    public void lookUpTest() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> socket1Stream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<TestBean> stream1 = socket1Stream
                .map(new MapFunction<String, TestBean>() {
                    @Override
                    public TestBean map(String s) throws Exception {
                        String[] eles = s.split(",");
                        TestBean testBean = new TestBean(
                                eles[0],
                                eles[1],
                                eles[2]
                        );
                        return testBean;
                    }
                });

        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE dic_test (\n" +
                        "  dic_code STRING,\n" +
                        "  dic_name STRING\n" +
//                        "  country STRING,\n" +
//                        "  zip STRING\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://dev1:3306/gmall',\n" +
                        "  'table-name' = 'base_dic',\n" +
                        "  'username' = 'hadoop',\n" +
                        "  'password' = '1vqpy7YopdzfxskPHb'\n" +
                        ")"
        );

        Table table1 = tableEnv.fromDataStream(stream1, $("id"), $("name"), $("addr"), $("ppp").proctime());
        tableEnv.createTemporaryView("table1", table1);

        tableEnv.executeSql("select table1.id,dic_name from table1 left join dic_test FOR SYSTEM_TIME AS OF table1.ppp on table1.id=dic_test.dic_code").print();

    }

    @Test
    public void ikTest() {
        List<String> words = KeywordUtil.analyze("张翔最讨厌侯凯毅");
        for (String word : words) {
            System.out.println(word);
        }
    }

    @Test
    public void lombokTest() {
        KeywordBean keywordBean = new KeywordBean("a", "b", "c", "d", 1l, 2l);
        System.out.println(keywordBean.getKeyword_count());
    }
}
