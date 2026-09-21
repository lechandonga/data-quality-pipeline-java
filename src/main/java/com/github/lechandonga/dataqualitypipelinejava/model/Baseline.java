package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 历史基线快照（不可变）。每个数据集一份，随成功提交的批次单调累积。
 */
public record Baseline(String dataset, long batchCount, Map<String, FieldStats> fieldStats) {

    public Baseline {
        fieldStats = new LinkedHashMap<>(fieldStats);
    }

    public static Baseline empty(String dataset) {
        return new Baseline(dataset, 0L, Map.of());
    }
}
