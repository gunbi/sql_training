package com.hanzhong.flink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;

public class NovelMySqlSink extends RichSinkFunction<Row> {
    private static final long serialVersionUID = 1L;
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private transient Connection connection;
    private transient PreparedStatement ps;

    public NovelMySqlSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        
        // 显式加载MySQL驱动
        Class.forName("com.mysql.cj.jdbc.Driver");
        
        // 创建连接
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        ps = connection.prepareStatement(
            "REPLACE INTO novels (id, create_time, category, novel_name, author_name, " +
            "author_level, update_time, word_count, monthly_ticket, total_click, status, complete_time) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        ps.setLong(1, (Long) row.getField(0));
        ps.setTimestamp(2, new java.sql.Timestamp(((Date) row.getField(1)).getTime()));
        ps.setString(3, (String) row.getField(2));
        ps.setString(4, (String) row.getField(3));
        ps.setString(5, (String) row.getField(4));
        ps.setString(6, (String) row.getField(5));
        ps.setTimestamp(7, new java.sql.Timestamp(((Date) row.getField(6)).getTime()));
        ps.setLong(8, (Long) row.getField(7));
        ps.setLong(9, (Long) row.getField(8));
        ps.setLong(10, (Long) row.getField(9));
        ps.setString(11, (String) row.getField(10));
        ps.setTimestamp(12, new java.sql.Timestamp(((Date) row.getField(11)).getTime()));
        ps.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (ps != null) {
            ps.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
} 