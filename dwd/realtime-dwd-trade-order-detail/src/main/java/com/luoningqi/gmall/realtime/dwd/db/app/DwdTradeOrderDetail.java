package com.luoningqi.gmall.realtime.dwd.db.app;

import com.luoningqi.gmall.realtime.common.base.BaseSQLApp;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class DwdTradeOrderDetail extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTradeOrderDetail().start(10024, 4, "dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //在flinkSQL里中使用join一定要添加状态的存活时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(5L));//存活时间
        //1.读取topic_db的数据
        createTopicDb(groupId, tableEnv);
        //2. 筛选订单详情表数据
        filterOd(tableEnv);
        //3.筛选订单信息表
        filterOi(tableEnv);
        //4.筛选订单详情活动关联表
        filterOda(tableEnv);
        //5.筛选订单详情优惠券关联表
        filterOdc(tableEnv);
        //6.将四张表格join合并
        Table joinTable = getJoinTable(tableEnv);
        //7.把合并结果写出到kafka中
        //一旦使用了left join会产生撤回流,此时如果需要将数据写出到kafka,不能使用一般的kafka sink,必须使用upsert kafka
        createUpsertKafkaSink(tableEnv);
        joinTable.insertInto(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL).execute();
    }

    public void createUpsertKafkaSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("create table " + Constant.TOPIC_DWD_TRADE_ORDER_DETAIL + "(\n" +
                "id STRING,  \n" +
                "order_id STRING, \n" +
                "sku_id STRING, \n" +
                "user_id STRING,\n" +
                "province_id STRING,\n" +
                "sku_name STRING,\n" +
                "order_price STRING, \n" +
                "sku_num STRING, \n" +
                "create_time STRING, \n" +
                "split_total_amount STRING, \n" +
                "split_activity_amount STRING, \n" +
                "split_coupon_amount STRING,\n" +
                "ts bigint,\n" +
                "PRIMARY KEY (id) NOT ENFORCED\n" +
                ")" + SQLUtil.getUpsertKafkaSQL(Constant.TOPIC_DWD_TRADE_ORDER_DETAIL));
    }

    public Table getJoinTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "od.id, \n" + // 订单详情的id
                "order_id, \n" + //订单的id
                "sku_id, \n" +
                "user_id,\n" +
                "province_id,\n" +
                "sku_name,\n" +
                "order_price, \n" +
                "sku_num, \n" +
                "create_time, \n" +
                "split_total_amount, \n" +
                "split_activity_amount, \n" +
                "split_coupon_amount,\n" +
                "ts\n" +
                "from order_detail od\n" +
                "join order_info oi\n" +
                "on od.order_id=oi.id\n" +
                "left join order_detail_activity oda\n" +
                "on od.id=oda.id\n" +
                "left join order_detail_coupon odc\n" +
                "on od.id=odc.id");
    }

    private static void filterOdc(StreamTableEnvironment tableEnv) {
        Table odcTable = tableEnv.sqlQuery("select\n" +
                "`data`['order_detail_id'] id,\n" +
                "`data`['coupon_id'] coupon_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_coupon'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_coupon", odcTable);
    }

    public void filterOda(StreamTableEnvironment tableEnv) {
        Table odaTable = tableEnv.sqlQuery("select\n" +
                "`data`['order_detail_id'] id,\n" +
                "`data`['activity_id'] activity_id,\n" +
                "`data`['activity_rule_id'] activity_rule_id\n" +
                "from topic_db\n" +
                "where `database`='gmall'\n" +
                "and `table`='order_detail_activity'\n" +
                "and `type`='insert'");
        tableEnv.createTemporaryView("order_detail_activity", odaTable);
    }

    public void filterOd(StreamTableEnvironment tableEnv) {
        Table odTable = tableEnv.sqlQuery("SELECT\n" +
                "  `data`['id'] AS id,\n" +
                "  `data`['order_id'] AS order_id,\n" +
                "  `data`['sku_id'] AS sku_id,\n" +
                "  `data`['sku_name'] AS sku_name,\n" +
                "  `data`['order_price'] AS order_price,\n" +
                "  `data`['sku_num'] AS sku_num,\n" +
                "  `data`['create_time'] AS create_time,\n" +
                "  `data`['split_total_amount'] AS split_total_amount,\n" +
                "  `data`['split_activity_amount'] AS split_activity_amount,\n" +
                "  `data`['split_coupon_amount'] AS split_coupon_amount,\n" +
                "  ts\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_detail'\n" +
                "AND `type` = 'insert';");
        tableEnv.createTemporaryView("order_detail", odTable);
    }

    public void filterOi(StreamTableEnvironment tableEnv) {
        Table oiTable = tableEnv.sqlQuery("SELECT\n" +
                "  `data`['id'] AS id,\n" +
                "  `data`['user_id'] AS user_id,\n" +
                "  `data`['province_id'] AS province_id\n" +
                "FROM topic_db\n" +
                "WHERE `database` = 'gmall'\n" +
                "AND `table` = 'order_info'\n" +
                "AND `type` = 'insert';");
        tableEnv.createTemporaryView("order_info", oiTable);
    }
}
