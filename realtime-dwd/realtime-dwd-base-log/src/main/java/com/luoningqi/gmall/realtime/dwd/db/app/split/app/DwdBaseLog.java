package com.luoningqi.gmall.realtime.dwd.db.app.split.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.base.BaseApp;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.DateFormatUtil;
import com.luoningqi.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//用户行为日志json 使用stream api(使用BaseApp基类)
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
//        stream.print();
        //核心业务处理
        //1.进行ETL过滤不完整的数据
        SingleOutputStreamOperator<JSONObject> JsonObjStream = etl(stream);
        //2.进行新旧访客的修复
        KeyedStream<JSONObject, String> keyedStream = keyByWithWaterMark(JsonObjStream);
        SingleOutputStreamOperator<JSONObject> isNewFixStream = isNewFix(keyedStream);
//        isNewFixStream.print();

        //3.拆分不同类型的用户行为日志
        // 启动日志: 启动信息 报错信息
        // 页面日志: 页面信息 曝光信息 动作信息 报错信息
        OutputTag<String> startTag = new OutputTag<String>("start", TypeInformation.of(String.class));
        OutputTag<String> errTag = new OutputTag<String>("err", TypeInformation.of(String.class));
        OutputTag<String> displayTag = new OutputTag<String>("display", TypeInformation.of(String.class));
        OutputTag<String> actionTag = new OutputTag<String>("action", TypeInformation.of(String.class));

        SingleOutputStreamOperator<String> pageStream = splitLog(isNewFixStream, errTag, startTag, displayTag, actionTag);
        SideOutputDataStream<String> startStream = pageStream.getSideOutput(startTag);
        SideOutputDataStream<String> errorStream = pageStream.getSideOutput(errTag);
        SideOutputDataStream<String> displayStream = pageStream.getSideOutput(displayTag);
        SideOutputDataStream<String> actionStream = pageStream.getSideOutput(actionTag);
        pageStream.print("page");
        startStream.print("start");
        errorStream.print("error");
        displayStream.print("display");
        actionStream.print("action");

        pageStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        startStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        errorStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        displayStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionStream.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }

    public SingleOutputStreamOperator<String> splitLog(SingleOutputStreamOperator<JSONObject> isNewFixStream, OutputTag<String> errTag, OutputTag<String> startTag, OutputTag<String> displayTag, OutputTag<String> actionTag) {
        return isNewFixStream.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> out) throws Exception {
                JSONObject err = value.getJSONObject("err");
                if (err != null) {
                    context.output(errTag, err.toJSONString());
                    value.remove("err");
                }
                JSONObject start = value.getJSONObject("start");
                JSONObject page = value.getJSONObject("page");
                JSONObject common = value.getJSONObject("common");
                Long ts = value.getLong("ts");

                if (start != null) {
                    //当前是启动日志
                    //注意,输出的是value完整的日志信息
                    context.output(startTag, value.toJSONString());
                } else if (page != null) {
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("ts", ts);
                            display.put("page", page);
                            context.output(displayTag, display.toJSONString());
                        }
                        value.remove("displays");
                    }

                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("ts", ts);
                            action.put("page", page);
                            context.output(actionTag, action.toJSONString());
                        }
                        value.remove("actions");
                    }

                    //只保持page信息,写出到主流
                    out.collect(value.toJSONString());

                } else {
                    //留空
                }

            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> isNewFix(KeyedStream<JSONObject, String> keyedStream) {
        return keyedStream.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            ValueState<String> firstLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //创造状态
                firstLoginDtState = getRuntimeContext().getState(new ValueStateDescriptor<>("first_login_dt", String.class));
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                //获取当前数据的is_new字段
                JSONObject common = value.getJSONObject("common");
                String isNew = common.getString("is_new");
                String firstLoginDt = firstLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                if ("1".equals(isNew)) {
//                    System.out.println(curDt);
//                    System.out.println(firstLoginDt);
                    //判断当前状态情况
                    if (firstLoginDt != null && !firstLoginDt.equals(curDt)) {
//                        System.out.println("10085");
                        //如果状态不为空,日期也不是今天,说明当前数据错误,不是新访客,伪装新访客
                        common.put("is_new", "0");
                    } else if (firstLoginDt == null) {
                        //状态为空 (说明确实是新访客第一次登陆)
//                        System.out.println("10086");
                        firstLoginDtState.update(curDt);
                    } else {
                        //留空,不做处理
                    }
                } else if ("0".equals(isNew)) {
                    //is_new为0
                    if (firstLoginDt == null) {
                        //老用户 flink实时数仓还没有记录过这个访客,需要补充访客的信息
                        //给访客首次登陆的日期补充一个值(今天以前的任意一天都可以)
                        firstLoginDtState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000L));
                    } else {
                        //留空
                        //正常情况不需要修复
                    }
                } else {
                    //is_new不为0也不为1是错误数据

                }
                out.collect(value);
            }
        });
    }

    public KeyedStream<JSONObject, String> keyByWithWaterMark(SingleOutputStreamOperator<JSONObject> JsonObjStream) {
//        return JsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(3L)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
//            @Override
//            public long extractTimestamp(JSONObject element, long recordTimestamp) {
//                return element.getLong("ts");
//            }
//        })).keyBy(new KeySelector<JSONObject, String>() {
//            @Override
//            public String getKey(JSONObject value) throws Exception {
//                return value.getJSONObject("common").getString("mid");
//            }
//        });
        return JsonObjStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        })).keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("mid");
            }
        });
    }

    public SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> stream) {
        return stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    JSONObject page = jsonObject.getJSONObject("page");
                    JSONObject start = jsonObject.getJSONObject("start");
                    Long ts = jsonObject.getLong("ts");
                    JSONObject common = jsonObject.getJSONObject("common");
                    if (page != null || start != null) {
                        if (common != null && common.getString("mid") != null && ts != null) {
                            out.collect(jsonObject);
                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    System.err.println("过滤掉脏数据" + value);
                }
            }
        });
    }
}
