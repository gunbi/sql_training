package com.hanzhong.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.types.Row;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Properties;

public class NovelProcessor {
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        // 禁用 JMX
        properties.setProperty("enable.jmx", "false");

        // 添加客户端 ID
        properties.setProperty("client.id", "flink-consumer-" + System.currentTimeMillis());

        // 设置超时时间
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("heartbeat.interval.ms", "10000");
        properties.setProperty("max.poll.interval.ms", "300000");
        properties.setProperty("auto.offset.reset", "earliest");

        // 设置反序列化
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 设置 Kafka 连接重试
        properties.setProperty("reconnect.backoff.ms", "1000");
        properties.setProperty("reconnect.backoff.max.ms", "10000");
        properties.setProperty("retry.backoff.ms", "1000");

        // 添加 DNS 查找配置
        properties.setProperty("client.dns.lookup", "use_all_dns_ips");

        System.out.println("Bootstrap servers: " + bootstrapServers);
        System.out.println("Kafka consumer properties: " + properties);
        // 使用新版本的 FlinkKafkaConsumer
        FlinkKafkaConsumer<String> consumer =
            new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
            );

        // 设置消费起始位置
        consumer.setStartFromEarliest();

        // 读取Kafka数据
        DataStream<String> stream = env.addSource(consumer).setParallelism(1).name("KafkaSource");

        // 处理数据
        DataStream<Row> resultStream = stream
            .map(new MapFunction<String, Novel>() {
                @Override
                public Novel map(String json) throws Exception {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(json);
                    return Novel.fromJsonNode(node);
                }
            })
            .filter(new FilterFunction<Novel>() {
                @Override
                public boolean filter(Novel novel) throws Exception {
                    return true;
                }
            })
            .map(new MapFunction<Novel, Row>() {
                @Override
                public Row map(Novel novel) throws Exception {
                    Row row = new Row(12);
                    row.setField(0, novel.getId());
                    row.setField(1, novel.getCreateTime());
                    row.setField(2, novel.getCategory());
                    row.setField(3, novel.getNovelName());
                    row.setField(4, novel.getAuthorName());
                    row.setField(5, novel.getAuthorLevel());
                    row.setField(6, novel.getUpdateTime());
                    row.setField(7, novel.getWordCount());
                    row.setField(8, novel.getMonthlyTicket());
                    row.setField(9, novel.getTotalClick());
                    row.setField(10, novel.getStatus());
                    row.setField(11, novel.getCompleteTime());
                    return row;
                }
            }).name("NovelProcessor");

        // 写入MySQL
        String jdbcUrl = String.format("jdbc:mysql://%s:%s/%s?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&useUnicode=true&characterEncoding=utf8",
            mysqlHost, mysqlPort, mysqlDatabase);

        resultStream.addSink(new MySqlSink(
            jdbcUrl,
            mysqlUser,
            mysqlPassword,
            "REPLACE INTO novels (id, create_time, category, novel_name, author_name, " +
            "author_level, update_time, word_count, monthly_ticket, total_click, status, complete_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )).name("MySqlSink").setParallelism(1);

        env.execute("Novel Processor");
    }
} 