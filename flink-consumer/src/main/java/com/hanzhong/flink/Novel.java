package com.hanzhong.flink;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import java.util.Date;

@Data
public class Novel {
    private long id;
    private Date createTime;
    private String category;
    private String novelName;
    private String authorName;
    private String authorLevel;
    private Date updateTime;
    private long wordCount;
    private long monthlyTicket;
    private long totalClick;
    private String status;
    private Date completeTime;

    public static Novel fromJsonNode(JsonNode node) {
        Novel novel = new Novel();
        novel.setId(node.get("id").asLong());
        novel.setCategory(node.get("category").asText());
        novel.setNovelName(node.get("novelName").asText());
        novel.setAuthorName(node.get("authorName").asText());
        novel.setAuthorLevel(node.get("authorLevel").asText());
        novel.setWordCount(node.get("wordCount").asLong());
        novel.setMonthlyTicket(node.get("monthlyTicket").asLong());
        novel.setTotalClick(node.get("totalClick").asLong());
        novel.setStatus(node.get("status").asText());

        // 处理日期字段
        JsonNode createTimeNode = node.get("createTime");
        if (createTimeNode != null && !createTimeNode.isNull()) {
            novel.setCreateTime(new Date(createTimeNode.asLong()));
        }

        JsonNode updateTimeNode = node.get("updateTime");
        if (updateTimeNode != null && !updateTimeNode.isNull()) {
            novel.setUpdateTime(new Date(updateTimeNode.asLong()));
        }

        JsonNode completeTimeNode = node.get("completeTime");
        if (completeTimeNode != null && !completeTimeNode.isNull()) {
            novel.setCompleteTime(new Date(completeTimeNode.asLong()));
        }

        return novel;
    }
} 