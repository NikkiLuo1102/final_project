import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class test03 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE topic_db (\n" +
                "`database` STRING,\n" +
                "`table` STRING,\n" +
                "`type` STRING, \n" +
                "`ts` BIGINT,\n" +
                "`data` MAP<STRING, STRING>,\n" +
                "`old` MAP<STRING, STRING>,\n" +
                " proc_time as PROCTIME()\n" +
                ") WITH (\n" +
                "'connector'='kafka',\n" +
                "'topic'='topic_db',\n" +
                "'properties.bootstrap.servers'='hadoop102:9092',\n" +
                "'properties.group.id'='test03',\n" +
                "'scan.startup.mode'='earliest-offset',\n" +
                "'format'='json'\n" +
                ")");
        tableEnv.executeSql("select TO_TIMESTAMP_LTZ(ts * 1000, 2) from topic_db;").print();

        tableEnv.executeSql("CREATE TABLE base_dic (\n" +
                " dic_node STRING,\n" +
                " dic_name STRING,\n" +
                " parent_code STRING,\n" +
                " PRIMARY KEY (dic_node) NOT ENFORCED\n" + // 修改这里的主键为 dic_node
                ") WITH (\n" +
                "'connector'='jdbc',\n" +
                "'url'='jdbc:mysql://hadoop102:3306/gmall',\n" +
                "'table_name' = 'base_dic',\n" +
                "'user_name' = 'root',\n" +
                "'pass_word' = '000000'\n" + // 去掉最后一个逗号
                ")");

        Table comment_info = tableEnv.sqlQuery("select\n" +
                        "`data`[id] id, \n" +
                        "`data`[user_id] user_id, \n" +
                        "`data`[nick_name] nick_name, \n" +
                        "`data`[head_img] head_img, \n" +
                        "`data`[sku_id] sku_id, \n" +
                        "`data`[spu_id] spu_id, \n" +
                        "`data`[order_id] order_id, \n" +
                        "`data`[appraise] appraise_code, \n" +
                        "b.dic_name appraise_name\n" +
                        "`data`[comment_txt] comment_txt, \n" +
                        "`data`[create_time] create_time, \n" +
                        "`data`[operate_time] operate_time,\n" +
                        " proc_time,\n"+
                        "FROM topic_db \n" +
                        "WHERE `database` = 'gmall'\n" +
                        "AND `table` = 'common_info'\n" +
                        "AND `type` = 'insert' "
        );

        tableEnv.createTemporaryView("comment_info", comment_info);

        Table joinTable = tableEnv.sqlQuery("select\n" +
                "id, \n" +
                "user_id, \n" +
                "nick_name, \n" +
                "head_img, \n" +
                "sku_id, \n" +
                "spu_id, \n" +
                "order_id, \n" +
                "appraise appraise_code, \n" +
                "b.dic_name appraise_name\n" +
                "comment_txt, \n" +
                "create_time, \n" +
                "operate_time,\n" +
                "from comment_info c\n" +
                "join base_dic FOR SYSTEM AS OF c.proc_time b\n" +
                "on c.appraise = b.dic_code"
        );

        joinTable.execute().print();


// 执行查询
        Table table1 = tableEnv.sqlQuery("SELECT * FROM base_dic");
// 打印查询结果
        table1.execute().print();

    }
}
