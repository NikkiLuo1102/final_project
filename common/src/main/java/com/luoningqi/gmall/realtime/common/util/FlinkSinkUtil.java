package com.luoningqi.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.luoningqi.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;
import java.util.Random;

public class FlinkSinkUtil {
    public static Sink<String> getKafkaSink(String topic) {

        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("luoningqi-" + topic + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    public static KafkaSink<JSONObject> getKafkaSinkWithTopicName() {
        return KafkaSink.<JSONObject>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(new KafkaRecordSerializationSchema<JSONObject>() {
                    @Nullable
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject element, KafkaRecordSerializationSchema.KafkaSinkContext
                            context, Long timestamp) {
                        String topicName = element.getString("sink_table");
                        element.remove("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topicName, Bytes.toBytes(element.toJSONString()));
                    }
                })
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("luoningqi-" + "base_db" + new Random().nextLong())
                .setProperty("transaction.timeout.ms", 15 * 60 * 1000 + "")
                .build();
    }

    public static DorisSink<String> getDorisSink(String tableName) {

        Properties properties = new Properties();
        //上游是json写入时,需要开启配置
        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "true");

        return  DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder() // 执行参数
                                .setLabelPrefix("label-doris" + System.currentTimeMillis())  // stream-load 导入数据时 label 的前缀
                                .setDeletable(false)
                                .setStreamLoadProp(properties)// 设置 stream load 的数据格式 默认是 csv,需要改成 json
                                .build()
                )
                .setSerializer(new SimpleStringSerializer())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes(Constant.FENODES)
                        .setTableIdentifier(Constant.DORIS_DATABASE+"."+tableName)
                        .setUsername(Constant.DORIS_USERNAME)
                        .setPassword(Constant.MYSQL_PASSWORD)
                        .build()
                )
                .build();
    }


}
