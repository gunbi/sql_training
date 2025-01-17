-- 创建 Kafka Source 表
CREATE TABLE kafka_novels (
    id BIGINT,
    createTime TIMESTAMP(3),
    category STRING,
    novelName STRING,
    authorName STRING,
    authorLevel STRING,
    updateTime TIMESTAMP(3),
    wordCount BIGINT,
    monthlyTicket BIGINT,
    totalClick BIGINT,
    status STRING,
    completeTime TIMESTAMP(3),
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'test-topic',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-sql-consumer',
    'format' = 'json',
    'scan.startup.mode' = 'earliest-offset'
);

-- 创建 MySQL Sink 表
CREATE TABLE mysql_novels (
    id BIGINT PRIMARY KEY,
    createTime TIMESTAMP(3),
    category STRING,
    novelName STRING,
    authorName STRING,
    authorLevel STRING,
    updateTime TIMESTAMP(3),
    wordCount BIGINT,
    monthlyTicket BIGINT,
    totalClick BIGINT,
    status STRING,
    completeTime TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://mysql:3306/novels',
    'table-name' = 'novels',
    'username' = 'flink',
    'password' = 'flink123'
);

-- 插入数据到 MySQL（过滤月票大于5的小说）
INSERT INTO mysql_novels
SELECT 
    id,
    createTime,
    category,
    novelName,
    authorName,
    authorLevel,
    updateTime,
    wordCount,
    monthlyTicket,
    totalClick,
    status,
    completeTime
FROM kafka_novels
WHERE monthlyTicket > 5;

-- 一些示例查询（可以在 Flink SQL Client 中执行）
-- 统计每个分类的小说数量
SELECT 
    category,
    COUNT(*) as novel_count,
    AVG(monthlyTicket) as avg_tickets
FROM kafka_novels
GROUP BY category;

-- 查找最受欢迎的作者
SELECT 
    authorName,
    COUNT(*) as novel_count,
    SUM(monthlyTicket) as total_tickets,
    SUM(totalClick) as total_clicks
FROM kafka_novels
GROUP BY authorName
ORDER BY total_tickets DESC
LIMIT 10;

-- 使用时间窗口统计
SELECT 
    TUMBLE_START(proc_time, INTERVAL '1' MINUTE) as window_start,
    category,
    COUNT(*) as novel_count,
    AVG(monthlyTicket) as avg_tickets
FROM kafka_novels
GROUP BY 
    TUMBLE(proc_time, INTERVAL '1' MINUTE),
    category; 