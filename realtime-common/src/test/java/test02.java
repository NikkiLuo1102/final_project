import com.luoningqi.gmall.realtime.common.constant.Constant;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test02 {
    public static void main(String[] args) {
        //1.构建flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

//      //2.添加检查点和状态后端参数
//        // 2.1 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//        // 2.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 2.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 2.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + "test01");
//        // 2.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 2.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 2.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 2.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //3.读取数据
//        KafkaSource<String> source = KafkaSource.<String>builder()
//                .setBootstrapServers("hadoop102:9092")
//                .setTopics("topic_db")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setGroupId("test10")
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();

        // 3.读取数据
        MySqlSource<String> source = MySqlSource.<String>builder()
                .serverTimeZone("Asia/Shanghai")
                .hostname(Constant.MYSQL_HOST)
                .port(Constant.MYSQL_PORT)
                .username(Constant.MYSQL_USER_NAME)
                .password(Constant.MYSQL_PASSWORD)
                .databaseList("gmall2023_config") // 添加需要监听的数据库
                .tableList("gmall2023_config.table_process_dim") // 添加需要监听的表
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> mysqlSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql_source");

        // 4.对数据源进行处理
        mysqlSource.print();

        // 5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}