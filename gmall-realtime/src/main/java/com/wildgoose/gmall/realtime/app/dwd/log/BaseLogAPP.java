package com.wildgoose.gmall.realtime.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wildgoose.gmall.realtime.utils.DateFormatUtil;
import com.wildgoose.gmall.realtime.utils.KafkaUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author 张翔
 * @date 2023/2/27 10:37
 * @description
 */
public class BaseLogAPP {

    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3); //生产环境设置为Kafka主题的分区数

//        //1.1 开启CheckPoint
//        env.enableCheckpointing(PropertiesUtil.getLong("flink.checkpoint.interval"), CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(PropertiesUtil.getLong("flink.checkpoint.timeout"));
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(PropertiesUtil.getInt("flink.checkpoint.max.num"));
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(PropertiesUtil.getInt("flink.checkpoint.restart.num"), PropertiesUtil.getLong("flink.checkpoint.restart.delay")));
//
//        //1.2 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(PropertiesUtil.get("flink.checkpoint.dir"));
//        System.setProperty("HADOOP_USER_NAME", PropertiesUtil.get("application.username"));

        //TODO 2.消费Kafka topic_log 主题的数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(KafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤掉非JSON格式的数据 & 将每行数据转换成JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });
        //获取侧输出流数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.print("Dirty>>>>>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> fixedStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<String> firstViewDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                firstViewDtState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastLoginDt", String.class));
            }

            @Override
            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                String firstViewDt = firstViewDtState.value();
                Long ts = jsonObject.getLong("ts");
                String dt = DateFormatUtil.toDate(ts);

                if ("1".equals(isNew)) {
                    if (firstViewDt == null) {
                        firstViewDtState.update(dt);
                    } else {
                        if (!firstViewDt.equals(dt)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (firstViewDt == null) {
                        // 将首次访问日期置为昨日
                        String yesterday = DateFormatUtil.toDate(ts - 1000 * 60 * 60 * 24);
                        firstViewDtState.update(yesterday);
                    }
                }

                collector.collect(jsonObject);

            }
        });

        //TODO 6.使用侧输出流进行分流处理，页面日志放到主流，启动、曝光、动作、错误放到侧输出流
        OutputTag<String> startTag = new OutputTag<String>("startTag"){};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag"){};
        OutputTag<String> actionTag = new OutputTag<String>("actionTag"){};
        OutputTag<String> errorTag = new OutputTag<String>("errorTag"){};
        SingleOutputStreamOperator<String> separatedStream = fixedStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {

                // 收集错误数据
                JSONObject error = jsonObject.getJSONObject("err");
                if (error != null) {
                    context.output(errorTag, jsonObject.toJSONString());
                }

                jsonObject.remove("err");

                // 收集启动数据
                JSONObject start = jsonObject.getJSONObject("start");
                if (start != null) {
                    context.output(startTag, jsonObject.toJSONString());
                } else {
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject common = jsonObject.getJSONObject("common");
                    Long ts = jsonObject.getLong("ts");

                    // 收集曝光数据
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            JSONObject displayObj = new JSONObject();
                            displayObj.put("display", display);
                            displayObj.put("common", common);
                            displayObj.put("page", page);
                            displayObj.put("ts", ts);
                            context.output(displayTag, displayObj.toJSONString());
                        }
                    }

                    // 收集动作数据
                    JSONArray actions = jsonObject.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            JSONObject actionObj = new JSONObject();
                            actionObj.put("action", action);
                            actionObj.put("common", common);
                            actionObj.put("page", page);
                            context.output(actionTag, actionObj.toJSONString());
                        }
                    }

                    // 收集页面数据
                    jsonObject.remove("displays");
                    jsonObject.remove("actions");
                    collector.collect(jsonObject.toJSONString());
                }
            }
        });

        //TODO 7.提取各个侧输出流数据
        DataStream<String> startDS = separatedStream.getSideOutput(startTag);
        DataStream<String> displayDS = separatedStream.getSideOutput(displayTag);
        DataStream<String> actionDS = separatedStream.getSideOutput(actionTag);
        DataStream<String> errorDS = separatedStream.getSideOutput(errorTag);

        //TODO 8.将数据打印并写入对应的主题
        startDS.print("Page>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");
        actionDS.print("Action>>>>>>>>>");
        errorDS.print("Error>>>>>>>>>");
        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        separatedStream.addSink(KafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(KafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(KafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(KafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(KafkaUtil.getFlinkKafkaProducer(error_topic));

        //TODO 9.启动任务
        env.execute();
    }
}
