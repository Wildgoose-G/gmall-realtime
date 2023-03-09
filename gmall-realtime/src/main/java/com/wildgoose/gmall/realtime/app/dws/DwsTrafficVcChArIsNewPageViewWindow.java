package com.wildgoose.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.bean.TrafficPageViewBean;
import com.wildgoose.gmall.realtime.utils.ClickHouseUtil;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 张翔
 * @date 2023/3/7 10:10
 * @description
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
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

        // TODO 2. 从kafka dwd_traffic_page_log 主题读取页面数据，封装为流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_channel_page_view_window";
        FlinkKafkaConsumer<String> kafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(topic, groupId);
        DataStreamSource<String> pageLogSource = env.addSource(kafkaConsumer);

        // TODO 3.转换页面流数据结构
        SingleOutputStreamOperator<JSONObject> jsonObjStream = pageLogSource.map(JSON::parseObject);

        // TODO 4.统计会话数、页面浏览数、页面访问时长，并封装为实体类
        SingleOutputStreamOperator<TrafficPageViewBean> mainStream = jsonObjStream.map(new MapFunction<JSONObject, TrafficPageViewBean>() {
            @Override
            public TrafficPageViewBean map(JSONObject jsonObject) throws Exception {
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");

                //获取ts
                Long ts = jsonObject.getLong("ts");

                //获取维度信息
                String vc = common.getString("vc");
                String ch = common.getString("ch");
                String ar = common.getString("ar");
                String isNew = common.getString("is_new");

                //获取页面访问时长
                Long duringTime = page.getLong("during_time");

                //定义变量接受其它度量值
                Long uvCt = 0L;
                Long svCt = 0L;
                Long pvCt = 0L;
                Long ujCt = 0L;

                //判断本页面是否开启了一个新的会话
                String lastPageId = page.getString("last_page_id");
                if (lastPageId == null) {
                    svCt = 1L;
                }

                //封装为实体类
                TrafficPageViewBean trafficPageViewBean = new TrafficPageViewBean(
                        "",
                        "",
                        vc,
                        ch,
                        ar,
                        isNew,
                        uvCt,
                        svCt,
                        pvCt,
                        duringTime,
                        ujCt,
                        ts
                );

                return trafficPageViewBean;
            }
        });

        // TODO 5. 从 Kafka dwd_traffic_user_jump_detail 读取跳出明细数据，封装为流
        String ujdTopic = "dwd_traffic_user_jump_detail";
        FlinkKafkaConsumer<String> ujdKafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(ujdTopic, groupId);
        DataStreamSource<String> ujdSource = env.addSource(ujdKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> ujdMappedStream =
                ujdSource.map(jsonStr -> {
                    JSONObject jsonObj = JSONObject.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts") + 10 * 1000L;

                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            0L,
                            0L,
                            0L,
                            0L,
                            1L,
                            ts
                    );
                });

        // TODO 6. 从 Kafka dwd_traffic_unique_visitor_detail 主题读取独立访客数据，封装为流
        String uvTopic = "dwd_traffic_unique_visitor_detail";
        FlinkKafkaConsumer<String> uvKafkaConsumer = KafkaUtil.getFlinkKafkaConsumer(uvTopic, groupId);
        DataStreamSource<String> uvSource = env.addSource(uvKafkaConsumer);
        SingleOutputStreamOperator<TrafficPageViewBean> uvMappedStream =
                uvSource.map(jsonStr -> {
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    JSONObject common = jsonObj.getJSONObject("common");
                    Long ts = jsonObj.getLong("ts");
                    // 获取维度信息
                    String vc = common.getString("vc");
                    String ch = common.getString("ch");
                    String ar = common.getString("ar");
                    String isNew = common.getString("is_new");

                    // 封装为实体类
                    return new TrafficPageViewBean(
                            "",
                            "",
                            vc,
                            ch,
                            ar,
                            isNew,
                            1L,
                            0L,
                            0L,
                            0L,
                            0L,
                            ts
                    );
                });

        // TODO 7.合并三条流
        DataStream<TrafficPageViewBean> pageViewBeanDS = mainStream
                .union(ujdMappedStream)
                .union(uvMappedStream);

        // TODO 8. 设置水位线
        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkStream = pageViewBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long recordTimestamp) {
                                        return trafficPageViewBean.getTs();
                                    }
                                }
                        )
        );

        // TODO 9. 按照维度分组
        KeyedStream<TrafficPageViewBean, Tuple4<String, String, String, String>> keyedBeanStream = withWatermarkStream.keyBy(trafficPageViewBean ->
                        Tuple4.of(
                                trafficPageViewBean.getVc(),
                                trafficPageViewBean.getCh(),
                                trafficPageViewBean.getAr(),
                                trafficPageViewBean.getIsNew()
                        )
                , Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING)
        );

        // TODO 10. 开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowStream = keyedBeanStream.window(TumblingEventTimeWindows.of(
                        Time.seconds(10L)))
                .allowedLateness(Time.seconds(10L));

        // TODO 11.聚合计算
        SingleOutputStreamOperator<TrafficPageViewBean> reduceStream =
                windowStream.reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setUjCt(value1.getUjCt() + value2.getUjCt());
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());

                                return value1;
                            }
                        },
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> input, Collector<TrafficPageViewBean> collector) throws Exception {

                                //获取数据
                                TrafficPageViewBean next = input.iterator().next();

                                System.out.println(">>>>>>>>>>>>>>" + next);

                                //补充信息
                                next.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                                next.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                                //修改TS
                                next.setTs(System.currentTimeMillis());

                                //输出数据

                            }
                        }
                );

        // TODO 12. 写入 OLAP 数据库
        reduceStream.addSink(ClickHouseUtil.<TrafficPageViewBean>getJdbcSink(
                "insert into dws_traffic_vc_ch_ar_is_new_page_view_window values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}
