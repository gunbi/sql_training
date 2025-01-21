#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# 设置环境变量
export FLINK_HOME=/Users/gunbi/flink-1.11.0
export PATH=$FLINK_HOME/bin:$PATH

FLINK_VERSION=1.11.0
KAFKA_VERSION=3.9.0
SCALA_VERSION=2.11

# 创建 lib 目录
mkdir -p "$SCRIPT_DIR/lib"

# 定义依赖列表
DEPENDENCIES=(
    # Flink Table & SQL
    "flink-json|org/apache/flink/flink-json/${FLINK_VERSION}/flink-json-${FLINK_VERSION}.jar"
    
    # Flink Kafka
    "flink-kafka|org/apache/flink/flink-connector-kafka_${SCALA_VERSION}/${FLINK_VERSION}/flink-connector-kafka_${SCALA_VERSION}-${FLINK_VERSION}.jar"
    "flink-kafka-base|org/apache/flink/flink-connector-kafka-base_${SCALA_VERSION}/${FLINK_VERSION}/flink-connector-kafka-base_${SCALA_VERSION}-${FLINK_VERSION}.jar"
    "kafka-clients|org/apache/kafka/kafka-clients/${KAFKA_VERSION}/kafka-clients-${KAFKA_VERSION}.jar"
    
    # Flink JDBC
    "flink-jdbc|org/apache/flink/flink-connector-jdbc_${SCALA_VERSION}/${FLINK_VERSION}/flink-connector-jdbc_${SCALA_VERSION}-${FLINK_VERSION}.jar"
    "mysql-connector|mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar"
)

# 下载依赖函数
download_if_not_exists() {
    local name=$1
    local path=$2
    local jar_name=$(basename "$path")
    local target="$SCRIPT_DIR/lib/$jar_name"
    
    if [ ! -f "$target" ]; then
        echo "Downloading $name..."
        wget -P "$SCRIPT_DIR/lib/" "https://repo.maven.apache.org/maven2/$path"
    else
        echo "$name already exists, skipping download"
    fi
}

# 下载所需依赖
for dep in "${DEPENDENCIES[@]}"; do
    IFS="|" read -r name path <<< "$dep"
    download_if_not_exists "$name" "$path"
done

# 启动 SQL Client
$FLINK_HOME/bin/sql-client.sh embedded \
    -d "$SCRIPT_DIR/conf/sql-client-conf.yaml" \
    -l "$SCRIPT_DIR/lib"