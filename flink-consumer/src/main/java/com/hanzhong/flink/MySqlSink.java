package com.hanzhong.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MySqlSink extends RichSinkFunction<Row> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String sql;

    private Connection connection;
    private PreparedStatement preparedStatement;
    private int batchCount = 0;

    public MySqlSink(String jdbcUrl, String username, String password, String sql) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.sql = sql.replace("INSERT INTO", "REPLACE INTO");
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection = DriverManager.getConnection(jdbcUrl, username, password);
            connection.setAutoCommit(false);
            preparedStatement = connection.prepareStatement(sql);
            System.out.println("Successfully connected to MySQL database");
        } catch (Exception e) {
            System.err.println("Error connecting to MySQL: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void invoke(Row row, Context context) throws Exception {
        try {
            for (int i = 0; i < row.getArity(); i++) {
                preparedStatement.setObject(i + 1, row.getField(i));
            }
            preparedStatement.addBatch();

            if (++batchCount % 100 == 0) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();
                } catch (Exception e) {
                    System.err.println("Error executing batch: " + e.getMessage());
                } finally {
                    preparedStatement.clearBatch();
                }
            }
        } catch (Exception e) {
            System.err.println("Error executing SQL: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (batchCount % 100 != 0) {
                try {
                    preparedStatement.executeBatch();
                    connection.commit();
                } catch (Exception e) {
                    System.err.println("Error executing final batch: " + e.getMessage());
                }
            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }
} 