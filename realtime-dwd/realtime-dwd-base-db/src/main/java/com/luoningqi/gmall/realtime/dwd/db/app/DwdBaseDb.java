package com.luoningqi.gmall.realtime.dwd.db.app;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.base.BaseApp;
import com.luoningqi.gmall.realtime.common.bean.TableProcessDwd;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.FlinkSinkUtil;
import com.luoningqi.gmall.realtime.common.util.FlinkSourceUtil;
import com.luoningqi.gmall.realtime.common.util.JdbcUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class DwdBaseDb extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseDb().start(10019, 4, "dwd_base_db", Constant.TOPIC_DB);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务逻辑
        //1.读取topic_db数据
//        stream.print();
        //2.清洗过滤和转化
        SingleOutputStreamOperator<JSONObject> jsonObjStream = flatMapToJsonObj(stream);
        //3.读取配置表数据，使用flinkCDC读取
        DataStreamSource<String> tableProcessDwd = env.fromSource(FlinkSourceUtil.getMySqlSource(Constant.PROCESS_DATABASE, Constant.PROCESS_DWD_TABLE_NAME), WatermarkStrategy.noWatermarks(), "table_process_dwd").setParallelism(1);
//        tableProcessDwd.print();
        //4.转换数据格式
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = flatMapToProcessDwd(tableProcessDwd);
//        processDwdStream.print();

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor = new MapStateDescriptor<>("process_state", String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastStream = processDwdStream.broadcast(mapStateDescriptor);
        //5.连接主流和广播流，对主流数据判断是否需要保留
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = connectAndProcessBaseDB(jsonObjStream, broadcastStream, mapStateDescriptor);

//        processStream.print();

        //6.筛选最后需要写出的字段
        SingleOutputStreamOperator<JSONObject> dataStream = filterColumns(processStream);

//        dataStream.print();

        //7.将多个表格的数据写到kafka同一个主题和不同主题都是可以的，这里采取的是不同主题
        dataStream.sinkTo(FlinkSinkUtil.getKafkaSinkWithTopicName());

    }

    public SingleOutputStreamOperator<JSONObject> filterColumns(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream) {
        SingleOutputStreamOperator<JSONObject> dataStream = processStream.map(new MapFunction<Tuple2<JSONObject, TableProcessDwd>, JSONObject>() {
            @Override
            public JSONObject map(Tuple2<JSONObject, TableProcessDwd> value) throws Exception {
                JSONObject jsonObject = value.f0;
                TableProcessDwd processDwd = value.f1;
                JSONObject data = jsonObject.getJSONObject("data");
                List<String> columns = Arrays.asList(processDwd.getSinkColumns().split(","));
                data.keySet().removeIf(key -> !columns.contains(key));
                data.put("sink_table", processDwd.getSinkTable());
                return data;
            }
        });
        return dataStream;
    }

    public SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> connectAndProcessBaseDB(SingleOutputStreamOperator<JSONObject> jsonObjStream, BroadcastStream<TableProcessDwd> broadcastStream, MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor) {
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> processStream = jsonObjStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject, TableProcessDwd>>() {
            final HashMap<String, TableProcessDwd> hashMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                Connection mysqlConnection = JdbcUtil.getMysqlConnection();
                List<TableProcessDwd> tableProcessDwds = JdbcUtil.queryList(mysqlConnection, "select * from gmall2023_config.table_process_dwd", TableProcessDwd.class, true);
                for (TableProcessDwd tableProcessDwd : tableProcessDwds) {
                    hashMap.put(tableProcessDwd.getSourceTable() + ":" + tableProcessDwd.getSourceType(), tableProcessDwd);
                }
            }

            /**
             * 将配置表数据存在广播状态中
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(TableProcessDwd value, Context ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String op = value.getOp();
                String key = value.getSourceTable() + ":" + value.getSourceType();
                if ("d".equals(op)) {
                    broadcastState.remove(key);
                    hashMap.remove(key);
                } else {
                    broadcastState.put(key, value);
                }

            }

            /**
             * 调用广播状态判断当前数据是否需要保留
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcessDwd>> out) throws Exception {
                String table = value.getString("table");
                String type = value.getString("type");
                String key = table + ":" + type;
                ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                TableProcessDwd processDwd = broadcastState.get("key");
                //二次判断 是否是先到的数据
                if (processDwd == null) {
                    processDwd = hashMap.get(key);
                }
                if (processDwd != null) {
                    out.collect(Tuple2.of(value, processDwd));
                }
            }
        }).setParallelism(1);
        return processStream;
    }

    public SingleOutputStreamOperator<TableProcessDwd> flatMapToProcessDwd(DataStreamSource<String> tableProcessDwd) {
        SingleOutputStreamOperator<TableProcessDwd> processDwdStream = tableProcessDwd.flatMap(new FlatMapFunction<String, TableProcessDwd>() {
            @Override
            public void flatMap(String value, Collector<TableProcessDwd> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    String op = jsonObject.getString("op");
                    TableProcessDwd processDwd;
                    if ("d".equals(op)) {
                        processDwd = jsonObject.getObject("before", TableProcessDwd.class);
                    } else {
                        processDwd = jsonObject.getObject("after", TableProcessDwd.class);
                    }
                    processDwd.setOp(op);
                    out.collect(processDwd);
                } catch (Exception e) {
                    System.out.println("捕获脏数据" + value);
                }
            }
        }).setParallelism(1);
        return processDwdStream;
    }

    public SingleOutputStreamOperator<JSONObject> flatMapToJsonObj(DataStreamSource<String> stream) {
        SingleOutputStreamOperator<JSONObject> jsonObjStream = stream.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    System.out.println("洗掉脏数据" + value);
                }
            }
        });
        return jsonObjStream;
    }
}
