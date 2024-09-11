package com.luoningqi.gmall.realtime.dws.app;

import com.luoningqi.gmall.realtime.common.base.BaseSQLApp;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.luoningqi.gmall.realtime.common.util.SQLUtil;
import com.luoningqi.gmall.realtime.dws.function.KwSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficSourceKeywordPageViewWindow().start(
                10021,
                4,
                "dws_traffic_source_keyword_page_view_window"
        );
    }

    @Override
    public void handle(StreamTableEnvironment tableEnv, StreamExecutionEnvironment env, String groupId) {
        //处理核心业务
        // 1. 读取主流DWD页面主题日志
        createPageInfo(tableEnv, groupId);

        // 2. 筛选出关键词keywords
        filterKeywords(tableEnv);

        // 3. 自定义UDTF分词函数,并注册
        tableEnv.createTemporaryFunction("KwSplit", KwSplit.class);

        // 4. 调用分词函数对keywords进行拆分
        KwSplit(tableEnv);

        // 5. 对keyword进行分组开窗聚合
        Table windowAggTable = getWindowAggTable(tableEnv);

        // 6. 写出到 doris 中
        //flink需要打开检查点才能把数据写出到doris
        createDorisSink(tableEnv);

        windowAggTable.insertInto("doris_sink").execute();
    }

    private static Table getWindowAggTable(StreamTableEnvironment tableEnv) {
        return tableEnv.sqlQuery("SELECT\n" +
                "CAST(TUMBLE_START(row_time, INTERVAL '10' SECOND) as STRING) stt,\n" +
                "CAST(TUMBLE_END(row_time, INTERVAL '10' SECOND) as STRING) edt,\n" +
                "CAST(CURRENT_DATE as STRING) cur_date,\n" +
                "keyword,\n" +
                "count(*) keyword_count,\n" +
                "FROM keyword_table\n" +
                "GROUP BY\n" +
                "TUMBLE(row_time, INTERVAL '10' SECOND),\n" +
                "keyword");
    }

    private static void createDorisSink(StreamTableEnvironment tableEnv) {
        tableEnv.executeSql("CREATE TABLE doris_sink(" +
                "  stt STRING, " +  // 2023-07-11 14:14:14
                "  edt STRING, " +
                "  cur_date STRING, " +
                "  keyword STRING, " +
                "  keyword_count BIGINT " +
                ") " + SQLUtil.getDorisSinkSQL(Constant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW));
    }

    private static void KwSplit(StreamTableEnvironment tableEnv) {
        Table keywordTable = tableEnv.sqlQuery("select keywords, keyword, `row_time`" +
                " from keywords_table " +
                " left join lateral table(KwSplit(myField)) on true ");

        tableEnv.createTemporaryView("keyword_table", keywordTable);
    }

    private static void filterKeywords(StreamTableEnvironment tableEnv) {
        Table keywordsTable = tableEnv.sqlQuery("select " +
                " page['item'] keywords, " +
                "`ts`" +
                "from page_info " +
                "where page['last_page_id'] ='search' " +
                "and page['item_type']='keyword' " +
                "and page['item'] is not null ");

        tableEnv.createTemporaryView("keywords_table", keywordsTable);
    }

    private static void createPageInfo(StreamTableEnvironment tableEnv, String groupId) {
        tableEnv.executeSql("create table page_info(" +
                " `common` map<string, string>, " +
                " `page` map<string, string>, " +
                " `ts` bigint, " +
                " `row_time` as TO_TIMESTAMP_LTZ(ts,3)," +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
                ")" + SQLUtil.getKafkaSourceSQL(Constant.TOPIC_DWD_TRAFFIC_PAGE, groupId));
    }

}
