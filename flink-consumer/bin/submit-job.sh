#!/bin/bash

# 获取脚本所在目录的绝对路径
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# 设置环境变量
export FLINK_HOME=/Users/hanzhong/flink-1.9.0
export PATH=$FLINK_HOME/bin:$PATH

# 参数配置
BOOTSTRAP_SERVERS=${1:-"localhost:29092"}
TOPIC=${2:-"test-topic"}
MYSQL_HOST=${3:-"localhost"}
MYSQL_PORT=${4:-"3306"}
MYSQL_DATABASE=${5:-"novels"}
MYSQL_USER=${6:-"flink"}
MYSQL_PASSWORD=${7:-"flink123"}

# 构建项目
echo "Building project..."
cd "$PROJECT_ROOT" && mvn clean package -DskipTests

# 检查构建结果
if [ ! -f "$PROJECT_ROOT/target/flink-consumer-1.0-SNAPSHOT.jar" ]; then
    echo "Build failed: JAR file not found"
    exit 1
fi

# 提交作业
echo "Submitting Flink job..."
$FLINK_HOME/bin/flink run \
    "$PROJECT_ROOT/target/flink-consumer-1.0-SNAPSHOT.jar" \
    --bootstrap.servers "$BOOTSTRAP_SERVERS" \
    --topic "$TOPIC" \
    --mysql.host "$MYSQL_HOST" \
    --mysql.port "$MYSQL_PORT" \
    --mysql.database "$MYSQL_DATABASE" \
    --mysql.user "$MYSQL_USER" \
    --mysql.password "$MYSQL_PASSWORD" 