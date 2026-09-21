package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 当前批次统计特征相对历史基线的一条漂移发现。
 */
public record DriftFinding(String field, DriftStatus status, String metric,
                           double baselineValue, double batchValue, String detail) {
}
