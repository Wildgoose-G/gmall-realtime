package com.wildgoose.gmall.realtime.app.dws;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.bean.TradePaymentWindowBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @author 张翔
 * @date 2023/3/7 16:38
 * @description
 */
public class DwsTradePaymentSucWindow {
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

        // TODO 3. 从 Kafka dwd_trade_pay_detail_suc 主题读取支付成功明细数据，封装为流
        String topic = "dwd_trade_pay_detail_suc";
        String groupId = "dws_trade_payment_suc_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 转换数据结构
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 5. 按照唯一键 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("order_detail_id"));

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastDataState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_data_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastData = lastDataState.value();
                        if (lastData == null) {
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastDataState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastData.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastDataState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject value = lastDataState.value();
                        if (value != null) {
                            out.collect(value);
                        }
                        lastDataState.clear();
                    }
                }
        );

        // TODO 7. 设置水位线
        // 经过两次 keyedProcessFunction 处理之后开窗，数据的时间语义会发生紊乱，可能会导致数据无法进入正确的窗口
        // 因此使用处理时间去重，在分组统计之前设置一次水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkSecondStream = processedStream.assignTimestampsAndWatermarks(
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
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkSecondStream.keyBy(r -> r.getString("user_id"));

        // TODO 9. 统计独立支付人数和新增支付人数
        SingleOutputStreamOperator<TradePaymentWindowBean> paymentWindowBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradePaymentWindowBean>() {

                    private ValueState<String> lastPaySucDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastPaySucDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_pay_suc_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradePaymentWindowBean> out) throws Exception {
                        String lastPaySucDt = lastPaySucDtState.value();
                        Long ts = jsonObj.getLong("ts") * 1000;
                        String paySucDt = DateFormatUtil.toDate(ts);

                        Long paymentSucUniqueUserCount = 0L;
                        Long paymentSucNewUserCount = 0L;

                        if (lastPaySucDt == null) {
                            paymentSucUniqueUserCount = 1L;
                            paymentSucNewUserCount = 1L;
                        } else {
                            if (!lastPaySucDt.equals(paySucDt)) {
                                paymentSucUniqueUserCount = 1L;
                            }
                        }
                        lastPaySucDtState.update(paySucDt);

                        TradePaymentWindowBean tradePaymentWindowBean = new TradePaymentWindowBean(
                                "",
                                "",
                                paymentSucUniqueUserCount,
                                paymentSucNewUserCount,
                                ts
                        );

                        long currentWatermark = ctx.timerService().currentWatermark();
                        out.collect(tradePaymentWindowBean);
                    }
                }
        );

        // TODO 10. 开窗
        AllWindowedStream<TradePaymentWindowBean, TimeWindow> windowDS = paymentWindowBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 11. 聚合
        SingleOutputStreamOperator<TradePaymentWindowBean> aggregatedDS = windowDS
                .aggregate(
                        new AggregateFunction<TradePaymentWindowBean, TradePaymentWindowBean, TradePaymentWindowBean>() {
                            @Override
                            public TradePaymentWindowBean createAccumulator() {
                                return new TradePaymentWindowBean(
                                        "",
                                        "",
                                        0L,
                                        0L,
                                        0L
                                );
                            }

                            @Override
                            public TradePaymentWindowBean add(TradePaymentWindowBean value, TradePaymentWindowBean accumulator) {
                                accumulator.setPaymentSucUniqueUserCount(
                                        accumulator.getPaymentSucUniqueUserCount() + value.getPaymentSucUniqueUserCount()
                                );
                                accumulator.setPaymentSucNewUserCount(
                                        accumulator.getPaymentSucNewUserCount() + value.getPaymentSucNewUserCount()
                                );
                                return accumulator;
                            }

                            @Override
                            public TradePaymentWindowBean getResult(TradePaymentWindowBean accumulator) {
                                return accumulator;
                            }

                            @Override
                            public TradePaymentWindowBean merge(TradePaymentWindowBean a, TradePaymentWindowBean b) {
                                return null;
                            }
                        },

                        new ProcessAllWindowFunction<TradePaymentWindowBean, TradePaymentWindowBean, TimeWindow>() {
                            @Override
                            public void process(Context context, Iterable<TradePaymentWindowBean> elements, Collector<TradePaymentWindowBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradePaymentWindowBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 12. 写出到 OLAP 数据库
        SinkFunction<TradePaymentWindowBean> jdbcSink = ClickHouseUtil.<TradePaymentWindowBean>getJdbcSink(
                "insert into dws_trade_payment_suc_window values(?,?,?,?,?)"
        );
        aggregatedDS.addSink(jdbcSink);

        env.execute();

    }
}
