package com.wildgoose.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.app.func.DimAsyncFunction;
import com.wildgoose.gmall.realtime.bean.TradeUserSpuOrderBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import com.wildgoose.gmall.realtime.utils.TimestampLtz3CompareUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author 张翔
 * @date 2023/3/8 17:00
 * @description
 */
public class DwsTradeUserSpuOrderWindow {

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

        // TODO 3. 从 Kafka dwd_trade_order_detail 主题读取下单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_trademark_category_user_order_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 4. 过滤字段不完整数据并转换数据结构
        SingleOutputStreamOperator<String> filteredDS = source.filter(
                new FilterFunction<String>() {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        return userId != null && sourceTypeName != null;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 5. 按照 order_detail_id 分组
        KeyedStream<JSONObject, String> keyedStream = mappedStream.keyBy(r -> r.getString("id"));

        // TODO 6. 去重
        SingleOutputStreamOperator<JSONObject> processedStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    private ValueState<JSONObject> lastValueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<JSONObject>("last_value_state", JSONObject.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastValue = lastValueState.value();
                        if (lastValue == null) {
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                            lastValueState.update(jsonObj);
                        } else {
                            String lastRowOpTs = lastValue.getString("row_op_ts");
                            String rowOpTs = jsonObj.getString("row_op_ts");
                            if (TimestampLtz3CompareUtil.compare(lastRowOpTs, rowOpTs) <= 0) {
                                lastValueState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<JSONObject> out) throws IOException {
                        JSONObject lastValue = this.lastValueState.value();
                        if (lastValue != null) {
                            out.collect(lastValue);
                        }
                        lastValueState.clear();
                    }
                }
        );

        // TODO 7. 转换数据结构
        SingleOutputStreamOperator<TradeUserSpuOrderBean> javaBeanStream = processedStream.map(
                jsonObj -> {
                    String orderId = jsonObj.getString("order_id");
                    String userId = jsonObj.getString("user_id");
                    String skuId = jsonObj.getString("sku_id");
                    Double splitTotalAmount = jsonObj.getDouble("split_total_amount");
                    Long ts = jsonObj.getLong("ts") * 1000L;
                    TradeUserSpuOrderBean trademarkCategoryUserOrderBean = TradeUserSpuOrderBean.builder()
                            .orderIdSet(new HashSet<String>(
                                    Collections.singleton(orderId)
                            ))
                            .userId(userId)
                            .skuId(skuId)
                            .orderAmount(splitTotalAmount)
                            .ts(ts)
                            .build();
                    return trademarkCategoryUserOrderBean;
                }
        );

        // TODO 8. 维度关联，补充分组维度字段
        // 关联 sku_info 表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withSkuInfoStream = AsyncDataStream.unorderedWait(
                javaBeanStream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_sku_info".toUpperCase()) {

                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkId(jsonObj.getString("tm_id".toUpperCase()));
                        javaBean.setCategory3Id(jsonObj.getString("category3_id".toUpperCase()));
                        javaBean.setSpuId(jsonObj.getString("spu_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getSkuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // TODO 9. 设置水位线
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withWatermarkDS = withSkuInfoStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TradeUserSpuOrderBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TradeUserSpuOrderBean>() {

                                    @Override
                                    public long extractTimestamp(TradeUserSpuOrderBean javaBean, long recordTimestamp) {
                                        return javaBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 10. 分组
        KeyedStream<TradeUserSpuOrderBean, String> keyedForAggregateStream = withWatermarkDS.keyBy(
                new KeySelector<TradeUserSpuOrderBean, String>() {

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) throws Exception {
                        return javaBean.getSpuId() + javaBean.getUserId();
                    }
                }
        );

        // TODO 11. 开窗
        WindowedStream<TradeUserSpuOrderBean, String, TimeWindow> windowDS = keyedForAggregateStream.window(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 12. 聚合
        SingleOutputStreamOperator<TradeUserSpuOrderBean> reducedStream = windowDS
                .reduce(
                        new ReduceFunction<TradeUserSpuOrderBean>() {
                            @Override
                            public TradeUserSpuOrderBean reduce(TradeUserSpuOrderBean value1, TradeUserSpuOrderBean value2) throws Exception {
                                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                                value1.setOrderAmount(value1.getOrderAmount() + value2.getOrderAmount());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TradeUserSpuOrderBean, TradeUserSpuOrderBean, String, TimeWindow>() {

                            @Override
                            public void process(String key, Context context, Iterable<TradeUserSpuOrderBean> elements, Collector<TradeUserSpuOrderBean> out) throws Exception {

                                String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                                String edt = DateFormatUtil.toYmdHms(context.window().getEnd());
                                for (TradeUserSpuOrderBean element : elements) {
                                    element.setStt(stt);
                                    element.setEdt(edt);
                                    element.setOrderCount((long) (element.getOrderIdSet().size()));
                                    element.setTs(System.currentTimeMillis());
                                    out.collect(element);
                                }
                            }
                        }
                );

        // TODO 13. 维度关联，补充与分组无关的维度字段
        // 13.1 关联 spu_info 表
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withSpuInfoStream = AsyncDataStream.unorderedWait(
                reducedStream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_spu_info".toUpperCase()) {
                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject dimJsonObj) throws Exception {
                        javaBean.setSpuName(
                                dimJsonObj.getString("spu_name".toUpperCase())
                        );
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getSpuId();
                    }
                },
                60 * 5, TimeUnit.SECONDS
        );

        // 13.2 关联品牌表 base_trademark
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withTrademarkStream = AsyncDataStream.unorderedWait(
                withSpuInfoStream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_base_trademark".toUpperCase()) {
                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setTrademarkName(jsonObj.getString("tm_name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getTrademarkId();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.3 关联三级分类表 base_category3
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withCategory3Stream = AsyncDataStream.unorderedWait(
                withTrademarkStream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_base_category3".toUpperCase()) {
                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory3Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory2Id(jsonObj.getString("category2_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getCategory3Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.4 关联二级分类表 base_category2
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withCategory2Stream = AsyncDataStream.unorderedWait(
                withCategory3Stream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_base_category2".toUpperCase()) {

                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory2Name(jsonObj.getString("name".toUpperCase()));
                        javaBean.setCategory1Id(jsonObj.getString("category1_id".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getCategory2Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // 13.5 关联一级分类表 base_category1
        SingleOutputStreamOperator<TradeUserSpuOrderBean> withCategory1Stream = AsyncDataStream.unorderedWait(
                withCategory2Stream,
                new DimAsyncFunction<TradeUserSpuOrderBean>("dim_base_category1".toUpperCase()) {
                    @Override
                    public void join(TradeUserSpuOrderBean javaBean, JSONObject jsonObj) throws Exception {
                        javaBean.setCategory1Name(jsonObj.getString("name".toUpperCase()));
                    }

                    @Override
                    public String getKey(TradeUserSpuOrderBean javaBean) {
                        return javaBean.getCategory1Id();
                    }
                },
                5 * 60, TimeUnit.SECONDS
        );

        // TODO 14. 写出到 OLAP 数据库
        SinkFunction<TradeUserSpuOrderBean> jdbcSink =
                ClickHouseUtil.<TradeUserSpuOrderBean>getJdbcSink(
                        "insert into dws_trade_user_spu_order_window values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                );
        withCategory1Stream.<TradeUserSpuOrderBean>addSink(jdbcSink);

        env.execute();
    }

}
