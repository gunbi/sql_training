# 使用 Maven 构建阶段
FROM maven:3.8-openjdk-8 AS builder

# 设置工作目录
WORKDIR /app

# 复制父项目的 pom.xml
COPY pom.xml .

# 复制子项目
COPY kafka-producer kafka-producer/

# 构建项目
RUN mvn -f kafka-producer/pom.xml clean package

# 运行阶段
FROM openjdk:8-jre-slim

WORKDIR /app

# 从构建阶段复制 jar 文件
COPY --from=builder /app/kafka-producer/target/*-with-dependencies.jar app.jar

# 创建数据目录
RUN mkdir -p /app/data

# 设置数据卷
VOLUME ["/app/data"]

# 设置环境变量
ENV BOOTSTRAP_SERVERS=localhost:9092 \
    TOPIC=test-topic \
    FILE_PATH=/app/data/input.csv \
    MESSAGES_PER_SECOND=1 \
    PARTITIONS=3 \
    REPLICATION_FACTOR=1

# 创建启动脚本
RUN echo '#!/bin/sh\n\
java -jar app.jar \
"$BOOTSTRAP_SERVERS" \
"$TOPIC" \
"$FILE_PATH" \
"$MESSAGES_PER_SECOND" \
"$PARTITIONS" \
"$REPLICATION_FACTOR"' > /app/start.sh && \
chmod +x /app/start.sh

# 启动命令
ENTRYPOINT ["/app/start.sh"] 