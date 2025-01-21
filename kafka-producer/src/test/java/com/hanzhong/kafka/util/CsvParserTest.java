package com.hanzhong.kafka.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class CsvParserTest {

    @TempDir
    Path tempDir;

    private File testFile;
    private CsvParser parser;

    @BeforeEach
    void setUp() throws IOException {
        // 创建测试CSV文件
        testFile = tempDir.resolve("test.csv").toFile();
        try (FileWriter writer = new FileWriter(testFile)) {
            writer.write("id,createTime,category,novelName,authorName,authorLevel,updateTime,wordCount,monthlyTicket,totalClick,status,completeTime\n");
            writer.write("7196,2012-03-12 09:51:04,历史,新三国策,晶晶亮,Lv.4,2023-03-25 09:20:37,65668,599,1136931,完本,2005-12-29 00:00:00\n");
            writer.write("7197,2012-03-12 09:51:04,都市,花开堪折,雪域倾情,,1970-01-01 08:00:00,,0,,,1970-01-01 08:00:00\n");
        }

        parser = new CsvParser(testFile.getAbsolutePath());
        parser.open();
    }

    @Test
    void testHeaderParsing() {
        String[] headers = parser.getHeaders();
        assertEquals(12, headers.length);
        assertEquals("id", headers[0]);
        assertEquals("createTime", headers[1]);
        assertEquals("category", headers[2]);
    }

    @Test
    void testParseNormalLine() throws IOException {
        ObjectNode node = parser.parseLine();
        assertNotNull(node);

        assertEquals(7196L, node.get("id").asLong());
        assertEquals("历史", node.get("category").asText());
        assertEquals("新三国策", node.get("novelName").asText());
        assertEquals("晶晶亮", node.get("authorName").asText());
        assertEquals("Lv.4", node.get("authorLevel").asText());
        assertEquals(65668L, node.get("wordCount").asLong());
        assertEquals(599L, node.get("monthlyTicket").asLong());
        assertEquals(1136931L, node.get("totalClick").asLong());
        assertEquals("完本", node.get("status").asText());

        // 验证日期字段
        assertTrue(node.get("createTime").isNumber());
        assertTrue(node.get("updateTime").isNumber());
        assertTrue(node.get("completeTime").isNumber());
        
        // 验证日期值的正确性
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        assertEquals("2012-03-12 09:51:04", 
            dateFormat.format(new Date(node.get("createTime").asLong())));
        assertEquals("2023-03-25 09:20:37", 
            dateFormat.format(new Date(node.get("updateTime").asLong())));
        assertEquals("2005-12-29 00:00:00", 
            dateFormat.format(new Date(node.get("completeTime").asLong())));
    }

    @Test
    void testParseLineWithEmptyFields() throws IOException {
        // 跳过第一行
        parser.parseLine();

        ObjectNode node = parser.parseLine();
        assertNotNull(node);

        assertEquals(7197L, node.get("id").asLong());
        assertEquals("都市", node.get("category").asText());
        assertEquals("花开堪折", node.get("novelName").asText());
        assertEquals("雪域倾情", node.get("authorName").asText());
        assertTrue(node.get("authorLevel").isNull() || node.get("authorLevel").asText().isEmpty());
        assertTrue(node.get("wordCount").isNull());
        assertEquals(0L, node.get("monthlyTicket").asLong());
        assertTrue(node.get("totalClick").isNull());
        assertTrue(node.get("status").isNull() || node.get("status").asText().isEmpty());
    }

    @Test
    void testReset() throws IOException {
        // 读取所有行
        while (parser.parseLine() != null) {
            // 继续读取
        }

        // 重置并验证可以重新读取
        parser.reset();
        ObjectNode firstNode = parser.parseLine();
        assertNotNull(firstNode);
        assertEquals(7196L, firstNode.get("id").asLong());
    }
} 