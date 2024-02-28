package com.luoningqi.gmall.realtime.dwd.db.split.app;

import com.luoningqi.gmall.realtime.common.base.BaseApp;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//用户行为日志json 使用stream api(使用BaseApp基类)
public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> stream) {
        //核心业务处理
    }
}
