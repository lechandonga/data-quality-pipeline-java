package com.github.lechandonga.dataqualitypipelinejava.baseline;

/**
 * 单字段数值分布的可复现统计特征。
 * 缺失率 + 数值均值/标准差 + 基数，全部基于确定聚合计算。
 */
public record BaselineStats(
        long count,
        long nonNullCount,
        long numericCount,
        double mean,
        double stdDev,
        long distinctCount) {

    public double nullRatio() {
        return count == 0 ? 0.0 : (double) (count - nonNullCount) / count;
    }
}
