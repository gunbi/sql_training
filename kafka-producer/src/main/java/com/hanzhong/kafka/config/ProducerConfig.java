package com.hanzhong.kafka.config;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ProducerConfig {

    private final String bootstrapServers;
    private final String topic;
    private final String filePath;
    private final int messagesPerSecond;
    private final int partitions;
    private final short replicationFactor;
} 