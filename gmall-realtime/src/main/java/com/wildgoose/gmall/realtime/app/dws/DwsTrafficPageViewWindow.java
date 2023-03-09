package com.wildgoose.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.bean.TrafficHomeDetailPageViewBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张翔
 * @date 2023/3/7 15:38
 * @description 流量域页面浏览各窗口汇总表
 */
public class DwsTrafficPageViewWindow {
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

        // TODO 2.读取 Kafka dwd_traffic_page_log 数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> source = env.addSource(kafkaConsumer);

        // TODO 3. 转换数据结构 String -> JSONObject
        SingleOutputStreamOperator<JSONObject> mappedStream = source.map(JSON::parseObject);

        // TODO 4. 过滤 page_id 不为 home && page_id 不为 good_detail 的数据
        SingleOutputStreamOperator<JSONObject> filteredStream = mappedStream.filter(
                jsonObj -> {
                    JSONObject page = jsonObj.getJSONObject("page");
                    String pageId = page.getString("page_id");
                    return pageId.equals("home") || pageId.equals("good_detail");
                });

        // TODO 5. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filteredStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 7. 按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = withWatermarkDS.keyBy(r -> r.getJSONObject("common").getString("mid"));

        // TODO 8. 鉴别独立访客，转换数据结构
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> uvStream = keyedStream.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {

                    private ValueState<String> homeLastVisitDt;
                    private ValueState<String> detailLastVisitDt;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        super.open(parameters);
                        homeLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("home_last_visit_dt", String.class)
                        );
                        detailLastVisitDt = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("detail_last_visit_dt", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {

                        String homeLastDt = homeLastVisitDt.value();
                        String detailLastDt = detailLastVisitDt.value();

                        JSONObject page = jsonObj.getJSONObject("page");
                        String pageId = page.getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String visitDt = DateFormatUtil.toDate(ts);

                        Long homeUvCt = 0L;
                        Long detailUvCt = 0L;

                        if (pageId.equals("home")) {
                            if (homeLastDt == null || !homeLastDt.equals(visitDt)) {
                                homeUvCt = 1L;
                                homeLastVisitDt.update(visitDt);
                            }
                        }

                        if (pageId.equals("good_detail")) {
                            if (detailLastDt == null || !detailLastDt.equals(visitDt)) {
                                detailUvCt = 1L;
                                detailLastVisitDt.update(visitDt);
                            }
                        }

                        if (homeUvCt != 0 || detailUvCt != 0) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "",
                                    "",
                                    homeUvCt,
                                    detailUvCt,
                                    0L
                            ));
                        }
                    }
                }
        );

        // TODO 9. 开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowStream = uvStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 10. 聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reducedStream =
                windowStream.reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                                value1.setGoodDetailUvCt(
                                        value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt()
                                );
                                value1.setHomeUvCt(
                                        value1.getHomeUvCt() + value2.getHomeUvCt()
                                );
                                return value1;
                            }
                        },
                        new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {

                            @Override
                            public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                                String stt = DateFormatUtil.toYmdHms(window.getStart());
                                String edt = DateFormatUtil.toYmdHms(window.getEnd());

                                for (TrafficHomeDetailPageViewBean value : values) {
                                    value.setStt(stt);
                                    value.setEdt(edt);
                                    value.setTs(System.currentTimeMillis());
                                    out.collect(value);
                                }
                            }
                        }
                );

        // TODO 11. 写出到 OLAP 数据库
        SinkFunction<TrafficHomeDetailPageViewBean> jdbcSink = ClickHouseUtil.<TrafficHomeDetailPageViewBean>getJdbcSink(
                "insert into dws_traffic_page_view_window values(?,?,?,?,?)"
        );
        reducedStream.<TrafficHomeDetailPageViewBean>addSink(jdbcSink);

        env.execute();
    }
}
