package com.hanzhong.kafka;

import com.hanzhong.kafka.config.ProducerConfig;
import com.hanzhong.kafka.util.CsvParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CsvKafkaProducer {
    private final ProducerConfig config;
    private final Producer<String, String> producer;
    private final ScheduledExecutorService executor;
    private CsvParser csvParser;

    public CsvKafkaProducer(ProducerConfig config) {
        this.config = config;
        createTopicIfNotExists();
        this.producer = createProducer();
        this.executor = Executors.newScheduledThreadPool(1);
        this.csvParser = new CsvParser(config.getFilePath());
    }

    private void createTopicIfNotExists() {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            boolean topicExists = adminClient.listTopics().names().get()
                .contains(config.getTopic());

            if (!topicExists) {
                log.info("创建topic: {}", config.getTopic());
                NewTopic newTopic = new NewTopic(
                    config.getTopic(),
                    config.getPartitions(),
                    config.getReplicationFactor()
                );

                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                log.info("Topic创建成功");
            } else {
                log.info("Topic已存在: {}", config.getTopic());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("创建topic失败: " + e.getMessage(), e);
        }
    }

    private Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);

        return new KafkaProducer<>(props);
    }

    private void reopenFile() throws IOException {
        if (csvParser != null) {
            csvParser.close();
        }
        csvParser = new CsvParser(config.getFilePath());
        csvParser.open();
    }

    public void start() {
        try {
            reopenFile();

            long intervalMs = 1000L / config.getMessagesPerSecond();

            executor.scheduleAtFixedRate(() -> {
                try {
                    ObjectNode data = csvParser.parseLine();
                    if (data == null) {
                        log.info("文件读取完毕，重新开始");
                        reopenFile();
                        return;
                    }

                    String json = data.toString();
                    String id = data.get("id").asText();

                    ProducerRecord<String, String> record =
                        new ProducerRecord<>(config.getTopic(), id, json);

                    producer.send(record, (metadata, exception) -> {
                        if (exception != null) {
                            log.error("发送消息失败: {}", exception.getMessage());
                        } else {
                            log.info("消息发送成功 - topic: {}, partition: {}, offset: {}, id: {}, data: {}",
                                metadata.topic(), metadata.partition(), metadata.offset(), id, json);
                        }
                    });
                } catch (Exception e) {
                    log.error("处理消息时发生错误: ", e);
                }
            }, 0, intervalMs, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.error("启动生产者时发生错误: {}", e.getMessage());
            stop();
        }
    }

    public void stop() {
        try {
            if (executor != null) {
                executor.shutdown();
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                    executor.shutdownNow();
                }
            }
            if (csvParser != null) {
                csvParser.close();
            }
            if (producer != null) {
                producer.flush();
                producer.close();
            }
        } catch (Exception e) {
            log.error("停止生产者时发生错误: {}", e.getMessage());
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            log.error("Usage: CsvKafkaProducer <bootstrapServers> <topic> <csvFilePath> <messagesPerSecond>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topic = args[1];
        String csvFilePath = args[2];
        int messagesPerSecond = Integer.parseInt(args[3]);

        ProducerConfig config = ProducerConfig.builder()
            .bootstrapServers(bootstrapServers)
            .topic(topic)
            .filePath(csvFilePath)
            .messagesPerSecond(messagesPerSecond)
            .partitions(3)  // 默认3个分区
            .replicationFactor((short) 1)  // 默认1个副本
            .build();

        CsvKafkaProducer producer = new CsvKafkaProducer(config);

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("正在关闭生产者...");
            producer.stop();
        }));

        try {
            log.info("启动生产者 - bootstrapServers: {}, topic: {}, csvFile: {}, messagesPerSecond: {}",
                bootstrapServers, topic, csvFilePath, messagesPerSecond);
            producer.start();
        } catch (Exception e) {
            log.error("生产者运行时发生错误: {}", e.getMessage());
            producer.stop();
            System.exit(1);
        }
    }
} 