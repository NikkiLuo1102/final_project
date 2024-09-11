package com.luoningqi.gmall.realtime.dwd.db.app;

import com.luoningqi.gmall.realtime.common.base.BaseSQLApp;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTradeCartAdd extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeCartAdd().start(10013, 4, "dwd_trade_cart_add");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //核心业务处理
        //1.读取topic_db数据
        createTopicDb(groupId, tableEnv);

        //2.筛选加购数据
        Table cartAddTable = filterCartAdd(tableEnv);

        //3.创建kafkaSink输出映射
        createKafkaSinkTable(tableEnv);

        //4.写出筛选的数据到对应的kafka主题
        cartAddTable.insertInto(Constant.TOPIC_DWD_TRADE_CART_ADD).execute();
    }

    public void createKafkaSinkTable(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_CART_ADD + "(" +
                "id string,\n" +
                "user_id string,\n" +
                "sku_id string,\n" +
                "cart_price string,\n" +
                "sku_num string,\n" +
                "sku_name string,\n" +
                "is_checked string,\n" +
                "create_time string,\n" +
                "operate_time string,\n" +
                "is_ordered string,\n" +
                "order_time string,\n" +
                "source_type string,\n" +
                "source_id string,\n" +
                "ts bigint" +
                ")" + SQLUtil.getKafkaSinkSQL(Constant.TOPIC_DWD_TRADE_CART_ADD));
    }

    public Table filterCartAdd(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("select\n" +
                "    `data`['id'] as id,\n" +
                "    `data`['user_id'] as user_id,\n" +
                "    `data`['sku_id'] as sku_id,\n" +
                "    `data`['cart_price'] as cart_price,\n" +
                "    if (`type`='insert',`data`['sku_num'], cast (cast(`data`['sku_num'] as BIGINT)-cast(`old`['sku_num'] as BIGINT) as STRING)) as sku_num,\n" +
                "    `data`['sku_name'] as sku_name,\n" +
                "    `data`['is_checked'] as is_checked,\n" +
                "    `data`['create_time'] as create_time,\n" +
                "    `data`['operate_time'] as operate_time,\n" +
                "    `data`['is_ordered'] as is_ordered,\n" +
                "    `data`['order_time'] as order_time,\n" +
                "    `data`['source_type'] as source_type,\n" +
                "    `data`['source_id'] as source_id,\n" +
                "    ts\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='cart_info'\n" +
                "and `type`='insert' or (\n" +
                "`type`='update' and (`type`='update' and `old`['sku_num'] is not null \n" +
                "and cast(`data`['sku_num'] as BIGINT)> cast(`old`['sku_num'] as BIGINT)))");
    }
}
