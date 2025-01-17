CREATE TABLE IF NOT EXISTS novels (
    id BIGINT PRIMARY KEY,
    create_time TIMESTAMP,
    category VARCHAR(50),
    novel_name VARCHAR(255),
    author_name VARCHAR(255),
    author_level VARCHAR(50),
    update_time TIMESTAMP,
    word_count BIGINT,
    monthly_ticket BIGINT,
    total_click BIGINT,
    status VARCHAR(50),
    complete_time TIMESTAMP
); 