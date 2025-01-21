package com.hanzhong.flink.operators;

import com.hanzhong.flink.Novel;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class NovelRowMapper implements MapFunction<Novel, Row> {
    private static final long serialVersionUID = 1L;

    @Override
    public Row map(Novel novel) throws Exception {
        Row row = new Row(12);
        row.setField(0, novel.getId());
        row.setField(1, novel.getCreateTime());
        row.setField(2, novel.getCategory());
        row.setField(3, novel.getNovelName());
        row.setField(4, novel.getAuthorName());
        row.setField(5, novel.getAuthorLevel());
        row.setField(6, novel.getUpdateTime());
        row.setField(7, novel.getWordCount());
        row.setField(8, novel.getMonthlyTicket());
        row.setField(9, novel.getTotalClick());
        row.setField(10, novel.getStatus());
        row.setField(11, novel.getCompleteTime());
        return row;
    }
} 