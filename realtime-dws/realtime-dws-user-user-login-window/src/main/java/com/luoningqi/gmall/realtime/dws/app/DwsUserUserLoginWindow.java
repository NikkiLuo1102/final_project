package com.luoningqi.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.base.BaseApp;
import com.luoningqi.gmall.realtime.common.bean.UserLoginBean;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.function.DorisMapFunction;
import com.luoningqi.gmall.realtime.common.util.DateFormatUtil;
import com.luoningqi.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024, 4, "DwsUserUserLoginWindow", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心逻辑
        //1.读取DWD页面上的主题数据
        stream.print();
        //2.对数据进行清洗过滤->uid不为空
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String uid = jsonObject.getJSONObject("common").getString("uid");
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    Long ts = jsonObject.getLong("ts");
                    if (uid != null && ts!=null && (lastPageId == null || "login".equals(lastPageId))) {
                        //当前为一次会话的第一条登陆数据
                        out.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("清洗脏数据" + value);
                }
            }
        });
        //3.注册水位线
        SingleOutputStreamOperator<JSONObject> withWaterMarkStream = jsonObjStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        })
        );
        //4.按照uid分组
        KeyedStream<JSONObject, String> keyedStream = withWaterMarkStream.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject value) throws Exception {
                return value.getJSONObject("common").getString("uid");
            }
        });
        //5.判断独立用户和回流用户
        SingleOutputStreamOperator<UserLoginBean> uuCtBeanStream = keyedStream.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> lastLoginDtDesc = new ValueStateDescriptor<>("last_login_dt", String.class);
                lastLoginDtState = getRuntimeContext().getState(lastLoginDtDesc);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                //比较当前登陆的日期和状态储存的日期
                String lastLoginDt = lastLoginDtState.value();
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.tsToDate(ts);
                //回流用户数
                Long backCt = 0L;
                //独立用户数
                Long uuCt = 0L;
//                if (lastLoginDt==null){
//                    //新的访客数据
//                    uuCt=1L;
//                    lastLoginDtState.update(curDt);
//                } else if (ts-DateFormatUtil.dateToTs(lastLoginDt)>7*24*60*60*1000L){
//                    //当前是回流用户
//                    backCt=1L;
//                    uuCt=1L;
//                    lastLoginDtState.update(curDt);
//                } else if (!lastLoginDt.equals(curDt)){
//                    //之前有登陆,但不是今天
//                    uuCt=1L;
//                    lastLoginDtState.update(curDt);
//                } else {
//                    //状态不为空,今天的又一次登陆
//                }
                //判断独立用户
                if (lastLoginDt == null || !lastLoginDt.equals(curDt)) {
                    uuCt = 1L;
                }
                //判断回流用户
                if (lastLoginDt == null && ts - DateFormatUtil.dateToTs(lastLoginDt) > 7 * 24 * 60 * 60 * 1000L) {
                    //当前是回流用户
                    backCt = 1L;
                }
                lastLoginDtState.update(curDt);
                //不是独立用户肯定不是回流用户,不需要下游统计
                if (uuCt != 0) {
                    out.collect(new UserLoginBean("", "", "", backCt, uuCt, ts));
                }
            }
        });

        //6.开窗聚合
        SingleOutputStreamOperator<Object> reduceBeanStream = uuCtBeanStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10L))).reduce(new ReduceFunction<UserLoginBean>() {
            @Override
            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                value1.setUuCt(value1.getUuCt() + value2.getUuCt());
                return value1;
            }
        }, new ProcessAllWindowFunction<UserLoginBean, Object, TimeWindow>() {
            @Override
            public void process(ProcessAllWindowFunction<UserLoginBean, Object, TimeWindow>.Context context, Iterable<UserLoginBean> elements, Collector<Object> out) throws Exception {
                TimeWindow window = context.window();
                String stt = DateFormatUtil.tsToDateTime(window.getStart());
                String edt = DateFormatUtil.tsToDateTime(window.getStart());
                String curDt = DateFormatUtil.tsToDateForPartition(System.currentTimeMillis());
                for (UserLoginBean element : elements) {
                    element.setStt(stt);
                    element.setEdt(edt);
                    element.setCurDate(curDt);
                    out.collect(element);
                }
            }
        });
        //7.写入doris
        reduceBeanStream.map(new DorisMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink(Constant.DWS_USER_USER_LOGIN_WINDOW));
    }
}
