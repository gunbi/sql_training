package com.hanzhong.flink;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import java.sql.Timestamp;
import java.time.Instant;

@Data
public class Novel {
    private long id;
    private Timestamp createTime;
    private String category;
    private String novelName;
    private String authorName;
    private String authorLevel;
    private Timestamp updateTime;
    private long wordCount;
    private long monthlyTicket;
    private long totalClick;
    private String status;
    private Timestamp completeTime;

    public static Novel fromJsonNode(JsonNode node) {
        Novel novel = new Novel();
        novel.setId(node.get("id").asLong());
        novel.setCreateTime(parseTimestamp(node.get("createTime").asText()));
        novel.setCategory(node.get("category").asText());
        novel.setNovelName(node.get("novelName").asText());
        novel.setAuthorName(node.get("authorName").asText());
        novel.setAuthorLevel(node.get("authorLevel").asText());
        novel.setUpdateTime(parseTimestamp(node.get("updateTime").asText()));
        novel.setWordCount(node.get("wordCount").asLong());
        novel.setMonthlyTicket(node.get("monthlyTicket").asLong());
        novel.setTotalClick(node.get("totalClick").asLong());
        novel.setStatus(node.get("status").asText());
        novel.setCompleteTime(parseTimestamp(node.get("completeTime").asText()));
        return novel;
    }

    private static Timestamp parseTimestamp(String dateStr) {
        try {
            return Timestamp.from(Instant.parse(dateStr));
        } catch (Exception e) {
            return null;
        }
    }
} 