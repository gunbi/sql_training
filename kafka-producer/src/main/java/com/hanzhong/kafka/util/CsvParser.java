package com.hanzhong.kafka.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class CsvParser {
    private final String filePath;
    private final String delimiter;
    private final ObjectMapper objectMapper;
    private String[] headers;
    private BufferedReader reader;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public CsvParser(String filePath) {
        this(filePath, ",");
    }

    public CsvParser(String filePath, String delimiter) {
        this.filePath = filePath;
        this.delimiter = delimiter;
        this.objectMapper = new ObjectMapper();
    }

    public void open() throws IOException {
        reader = new BufferedReader(new FileReader(filePath));
        // 读取并解析头部
        String headerLine = reader.readLine();
        if (headerLine != null) {
            headers = parseHeaderLine(headerLine);
            log.info("CSV headers: {}", String.join(", ", headers));
        } else {
            throw new IOException("CSV文件为空");
        }
    }

    private String[] parseHeaderLine(String headerLine) {
        // 处理可能的ID前缀，如 "id|id" -> "id"
        String[] rawHeaders = headerLine.split(delimiter);
        String[] cleanHeaders = new String[rawHeaders.length];

        for (int i = 0; i < rawHeaders.length; i++) {
            String header = rawHeaders[i];
            // 如果字段包含"|"，取最后一部分
            if (header.contains("|")) {
                String[] parts = header.split("\\|");
                cleanHeaders[i] = parts[parts.length - 1].trim();
            } else {
                cleanHeaders[i] = header.trim();
            }
        }
        return cleanHeaders;
    }

    public ObjectNode parseLine() throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }

        String[] values = line.split(delimiter);
        ObjectNode node = objectMapper.createObjectNode();

        for (int i = 0; i < Math.min(headers.length, values.length); i++) {
            String value = values[i].trim();
            String header = headers[i];

            // 处理ID字段的特殊情况
            if (i == 0 && value.contains("|")) {
                String[] parts = value.split("\\|");
                value = parts[parts.length - 1].trim();
            }

            if (value.isEmpty()) {
                node.putNull(header);
                continue;
            }

            // 处理日期字段
            if (isDateField(header)) {
                try {
                    Date date = DATE_FORMAT.parse(value);
                    node.put(header, date.getTime());
                } catch (ParseException e) {
                    log.warn("解析日期失败: {} = {}", header, value);
                    node.putNull(header);
                }
                continue;
            }

            // 处理其他字段
            try {
                // 尝试解析为Long
                node.put(header, Long.parseLong(value));
            } catch (NumberFormatException e1) {
                try {
                    // 尝试解析为Double
                    node.put(header, Double.parseDouble(value));
                } catch (NumberFormatException e2) {
                    // 如果都失败，则作为字符串处理
                    node.put(header, value);
                }
            }
        }

        return node;
    }

    private boolean isDateField(String fieldName) {
        return fieldName.equals("createTime") || 
               fieldName.equals("updateTime") || 
               fieldName.equals("completeTime");
    }

    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    public void reset() throws IOException {
        close();
        open();
    }

    public String[] getHeaders() {
        return headers;
    }
} 