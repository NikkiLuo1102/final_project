import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test01 {
    public static void main(String[] args) {
        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 luoningqi
//        System.setProperty("HADOOP_USER_NAME", "luoningqi");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.3 设置并行度
        env.setParallelism(4);

        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
//
//        // 1.4.2 开启 checkpoint
//        env.enableCheckpointing(5000);
//        // 1.4.3 设置 checkpoint 模式: 精准一次
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        // 1.4.4 checkpoint 存储
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall2023/stream/" + "test01");
//        // 1.4.5 checkpoint 并发数
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        // 1.4.6 checkpoint 之间的最小间隔
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
//        // 1.4.7 checkpoint  的超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        // 1.4.8 job 取消时 checkpoint 保留策略
//        env.getCheckpointConfig().setExternalizedCheckpointCleanup(RETAIN_ON_CANCELLATION);

        //3.读取数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaSource.<String>builder()
                        .setBootstrapServers("hadoop102:9092")
                        .setTopics("topic_db")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setGroupId("dim_app")
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .build(), WatermarkStrategy.<String>noWatermarks(), "kafka_source");

        //4.对数据源进行处理
        kafkaSource.print();

        //5.执行环境
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
