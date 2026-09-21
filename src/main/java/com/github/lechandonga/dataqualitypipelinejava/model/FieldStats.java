package com.github.lechandonga.dataqualitypipelinejava.model;

/**
 * 单个字段在一个批次/基线上的统计特征。
 * count 为参与统计的取值数；mean/variance 为数值字段的均值与总体方差；
 * nullCount 记录空值数，用于缺失率漂移；distinct 为基数估计容量上界内的精确值。
 */
public record FieldStats(long count, long nullCount, double mean, double variance,
                         double min, double max, long distinct) {

    public static final FieldStats EMPTY = new FieldStats(0, 0, 0.0, 0.0,
            Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0L);

    public double stdDev() {
        return Math.sqrt(Math.max(0.0, variance));
    }
}
