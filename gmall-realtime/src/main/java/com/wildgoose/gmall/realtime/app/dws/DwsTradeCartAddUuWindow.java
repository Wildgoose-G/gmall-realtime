package com.wildgoose.gmall.realtime.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.bean.CartAddUuBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * @author 张翔
 * @date 2023/3/7 16:17
 * @description
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        // TODO 3. 从 Kafka dwd_trade_cart_add 主题读取数据
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // TODO 6. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getString("user_id"));

        // TODO 7. 筛选加购独立用户
        SingleOutputStreamOperator<JSONObject> filteredStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<String> lastCartAddDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastCartAddDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_cart_add_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        String lastCartAdd = lastCartAddDt.value();

                        String cartAddDt = DateFormatUtil.toDate(jsonObj.getLong("ts") * 1000L);
                        if (lastCartAdd == null || !lastCartAdd.equals(cartAddDt)) {
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // TODO 8. 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = filteredStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 9. 聚合
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject jsonObj, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());
                        for (Long value : values) {
                            CartAddUuBean cartAddUuBean = new CartAddUuBean(
                                    stt,
                                    edt,
                                    value,
                                    System.currentTimeMillis()
                            );
                            out.collect(cartAddUuBean);
                        }
                    }
                }
        );

        // TODO 10. 写入到 OLAP 数据库
        SinkFunction<CartAddUuBean> jdbcSink = ClickHouseUtil.<CartAddUuBean>getJdbcSink(
                "insert into dws_trade_cart_add_uu_window values(?,?,?,?)"
        );
        aggregateDS.<CartAddUuBean>addSink(jdbcSink);

        env.execute();

    }
}
