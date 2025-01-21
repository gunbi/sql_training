package com.hanzhong.flink.operators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hanzhong.flink.Novel;
import org.apache.flink.api.common.functions.MapFunction;

public class NovelJsonMapper implements MapFunction<String, Novel> {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Novel map(String json) throws Exception {
        JsonNode node = mapper.readTree(json);
        return Novel.fromJsonNode(node);
    }
} 