# Flink SQL Training Project

这是一个用于学习和实践Flink SQL的示例项目。本项目提供了完整的本地开发环境设置和多个Flink SQL使用示例。

## 环境要求

- Docker & Docker Compose
- Apache Flink 1.9
- Java 8+

## 快速开始

### 1. 环境准备

首先确保已经安装了所需的软件：
- Docker 和 Docker Compose
- Apache Flink 1.9（本地安装）

### 2. 启动依赖服务

本项目使用Docker Compose启动必要的依赖服务：

```bash docker-compose up -d```

这将启动：
- Kafka（包含自动的数据生产者）
- MySQL
- Zookeeper

### 3. 启动Flink集群

使用本地Flink的启动脚本启动集群：

```bash ${FLINK_HOME}/bin/start-cluster.sh```

### 4. 运行Flink SQL示例

本项目提供了两种运行Flink作业的方式：
注意：使用前请修改脚本中的`FLINK_HOME`变量为你的本地Flink安装路径。

#### 方式一：使用Flink SQL Client
```bash ./flink-consumer/bin/start-sql-client.sh```

#### 方式二：提交Flink JAR作业
```bash ./flink-consumer/bin/submit-job.sh```


## 注意事项

1. 确保所有脚本中的`FLINK_HOME`变量都已正确设置为本地Flink安装目录
2. 运行Docker Compose之前，确保没有端口冲突
3. 可以通过Flink Web UI（默认地址：http://localhost:8081）监控作业运行状态

## 问题排查

如果遇到问题，请检查：
1. Docker容器是否正常运行
2. Flink集群是否正常启动
3. 环境变量是否正确设置

## 贡献指南


## 许可证


