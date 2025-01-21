package com.hanzhong.flink;

import com.hanzhong.flink.operators.NovelCompleteFilter;
import com.hanzhong.flink.operators.NovelJsonMapper;
import com.hanzhong.flink.operators.NovelRowMapper;
import java.io.Serializable;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;

public class NovelProcessor implements Serializable {

    private static final long serialVersionUID = 1L;

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置时间特性为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 从参数中获取配置
        String bootstrapServers = params.get("bootstrap.servers", "localhost:9092");
        String topic = params.get("topic", "test-topic");
        String mysqlHost = params.get("mysql.host", "localhost");
        String mysqlPort = params.get("mysql.port", "3306");
        String mysqlDatabase = params.get("mysql.database", "novels");
        String mysqlUser = params.get("mysql.user", "flink");
        String mysqlPassword = params.get("mysql.password", "flink123");

        // Kafka consumer 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "novel-processor-group");

        // 移除这些配置，让Flink自己管理
        // properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.setProperty("enable.auto.commit", "true");
        // properties.setProperty("auto.commit.interval.ms", "1000");
        // properties.setProperty("auto.offset.reset", "earliest");

        // 保留必要的配置
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("heartbeat.interval.ms", "10000");
        properties.setProperty("max.poll.interval.ms", "300000");

        // 设置 Kafka 连接重试
        properties.setProperty("reconnect.backoff.ms", "1000");
        properties.setProperty("reconnect.backoff.max.ms", "10000");
        properties.setProperty("retry.backoff.ms", "1000");

        // 添加 DNS 查找配置
        properties.setProperty("client.dns.lookup", "use_all_dns_ips");

        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Kafka consumer properties: " + properties);

        // 创建Flink Kafka消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            topic,
            new SimpleStringSchema(),
            properties
        );

        // 设置消费起始位置
        consumer.setStartFromEarliest();

        // 读取Kafka数据
        DataStream<String> stream = env.addSource(consumer).setParallelism(1).name("KafkaSource");

        // 处理数据并设置EventTime
        DataStream<Row> resultStream = stream
            .map(new NovelJsonMapper())
            .assignTimestampsAndWatermarks(WatermarkStrategy
                .<Novel>forBoundedOutOfOrderness(Duration.ofSeconds(300000))
                .withTimestampAssigner(
                    (SerializableTimestampAssigner<Novel>) (element, recordTimestamp)
                        -> element.getCompleteTime() != null ? element.getCompleteTime().getTime() : 0L
                )
                .withIdleness(Duration.ofSeconds(300000)))
            .filter(new NovelCompleteFilter())
            .map(new NovelRowMapper())
            .name("NovelProcessor");

        // 写入MySQL
        String jdbcUrl = String.format(
            "jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8",
            mysqlHost, mysqlPort, mysqlDatabase);

        resultStream.addSink(new NovelMySqlSink(jdbcUrl, mysqlUser, mysqlPassword))
            .name("NovelMySqlSink")
            .setParallelism(1);

        env.execute("Novel Processor");
    }
}