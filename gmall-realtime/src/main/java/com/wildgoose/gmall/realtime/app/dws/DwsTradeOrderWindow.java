package com.wildgoose.gmall.realtime.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.wildgoose.gmall.realtime.bean.TradeOrderBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
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

import java.util.Objects;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * @author 张翔
 * @date 2023/3/7 16:42
 * @description
 */
public class DwsTradeOrderWindow {
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

        // TODO 3. 从 Kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {

                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        if (userId != null && sourceTypeName != null) {
                            return true;
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 对 order_detail_id 相同的数据去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                            private ValueState<JSONObject> filterState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                filterState = getRuntimeContext().getState(
                                        new ValueStateDescriptor<JSONObject>("filter_state", JSONObject.class)
                                );
                            }

                            @Override
                            public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                                JSONObject lastData = filterState.value();

                                if (lastData == null) {
                                    long currentProcessingTime = ctx.timerService().currentProcessingTime();
                                    ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                                    filterState.update(jsonObj);
                                } else {
                                    String lastRowOpTs = lastData.getString("row_op_ts");
                                    String rowOpTs = jsonObj.getString("row_op_ts");
                                    if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                        filterState.update(jsonObj);
                                    }
                                }

                            }

                            @Override
                            public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                                super.onTimer(timestamp, ctx, out);
                                JSONObject currentValue = filterState.value();
                                if (currentValue != null) {
                                    out.collect(currentValue);
                                }
                                filterState.clear();
                            }
                        }
                );

        // TODO 7. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = processedStream.assignTimestampsAndWatermarks(
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

        // TODO 8. 按照用户 id 分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkStream.keyBy(r -> r.getString("user_id"));

        // TODO 9. 统计当日下单独立用户数和新增下单用户数
        SingleOutputStreamOperator<TradeOrderBean> orderBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();
                        String orderDt = jsonObj.getString("date_id");

                        Long orderNewUserCount = 0L;
                        Long orderUniqueUserCount = 0L;
                        Double splitActivityAmount = jsonObj.getDouble("split_activity_amount");
                        Double splitCouponAmount = jsonObj.getDouble("split_coupon_amount");
                        Double splitOriginalAmount = jsonObj.getDouble("split_original_amount");
                        Long ts = jsonObj.getLong("ts") * 1000L;

                        if (lastOrderDt == null) {
                            orderNewUserCount = 1L;
                            orderUniqueUserCount = 1L;
                        } else {
                            if (!lastOrderDt.equals(orderDt)) {
                                orderUniqueUserCount = 1L;
                            }
                        }
                        lastOrderDtState.update(orderDt);

                        TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                "",
                                "",
                                orderUniqueUserCount,
                                orderNewUserCount,
                                splitActivityAmount,
                                splitCouponAmount,
                                splitOriginalAmount,
                                ts
                        );

                        out.collect(tradeOrderBean);
                    }
                }
        );

        // TODO 10. 开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = orderBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 11. 聚合
        SingleOutputStreamOperator<TradeOrderBean> aggregatedStream = windowDS.aggregate(
                new AggregateFunction<TradeOrderBean, TradeOrderBean, TradeOrderBean>() {

                    @Override
                    public TradeOrderBean createAccumulator() {
                        return new TradeOrderBean(
                                "",
                                "",
                                0L,
                                0L,
                                0.0,
                                0.0,
                                0.0,
                                0L
                        );
                    }

                    @Override
                    public TradeOrderBean add(TradeOrderBean value, TradeOrderBean accumulator) {
                        accumulator.setOrderUniqueUserCount(
                                accumulator.getOrderUniqueUserCount() + value.getOrderUniqueUserCount()
                        );
                        accumulator.setOrderNewUserCount(
                                accumulator.getOrderNewUserCount() + value.getOrderNewUserCount()
                        );
                        accumulator.setOrderActivityReduceAmount(
                                accumulator.getOrderActivityReduceAmount() +
                                        (value.getOrderActivityReduceAmount() == null
                                                ? 0.0 : value.getOrderActivityReduceAmount()
                                        )
                        );
                        accumulator.setOrderCouponReduceAmount(
                                accumulator.getOrderCouponReduceAmount() +
                                        (value.getOrderCouponReduceAmount() == null
                                                ? 0.0 : value.getOrderCouponReduceAmount()
                                        )
                        );
                        accumulator.setOrderOriginalTotalAmount(
                                accumulator.getOrderOriginalTotalAmount() +
                                        (value.getOrderOriginalTotalAmount() == null
                                                ? 0.0 : value.getOrderOriginalTotalAmount()
                                        )
                        );
                        return accumulator;
                    }

                    @Override
                    public TradeOrderBean getResult(TradeOrderBean accumulator) {
                        return accumulator;
                    }

                    @Override
                    public TradeOrderBean merge(TradeOrderBean a, TradeOrderBean b) {
                        return null;
                    }
                },
                new AllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow window, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(window.getStart());
                        String edt = DateFormatUtil.toYmdHms(window.getEnd());
                        for (TradeOrderBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );

        // TODO 12. 写出到 OLAP 数据库
        SinkFunction<TradeOrderBean> jdbcSink = ClickHouseUtil.<TradeOrderBean>getJdbcSink(
                "insert into dws_trade_order_window values(?,?,?,?,?,?,?,?)"
        );
        aggregatedStream.<TradeOrderBean>addSink(jdbcSink);

        env.execute();

    }
}
