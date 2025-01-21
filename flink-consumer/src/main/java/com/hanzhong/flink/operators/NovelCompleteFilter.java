package com.hanzhong.flink.operators;

import com.hanzhong.flink.Novel;
import org.apache.flink.api.common.functions.FilterFunction;

public class NovelCompleteFilter implements FilterFunction<Novel> {
    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(Novel novel) throws Exception {
        return novel.getCompleteTime() != null;
    }
} 