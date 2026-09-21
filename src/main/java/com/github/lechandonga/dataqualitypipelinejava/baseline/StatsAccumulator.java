package com.github.lechandonga.dataqualitypipelinejava.baseline;

import java.util.HashSet;
import java.util.Set;

/**
 * 流式统计累加器：单遍扫描，不保存全部原始值即可计算均值与标准差。
 *
 * <p>均值/方差采用 Welford 在线算法；distinct 采用有界基数集合
 * （由 distinctCapacity 限制），超出后停止精确跟踪基数，
 * 避免大输入无界占用内存。</p>
 *
 * <p>统计结果只依赖输入多重集（每条记录最终被计入一次），
 * 与消费顺序、并发度无关，保证重复执行可复现。</p>
 */
public final class StatsAccumulator {

    private final int distinctCapacity;
    private long totalCount;
    private long nonNullCount;
    private long numericCount;
    private double mean;
    private double m2;
    private final Set<String> distinct;
    private boolean distinctTruncated;

    public StatsAccumulator() {
        this(100_000);
    }

    public StatsAccumulator(int distinctCapacity) {
        if (distinctCapacity <= 0) {
            throw new IllegalArgumentException("distinctCapacity 必须为正数");
        }
        this.distinctCapacity = distinctCapacity;
        this.distinct = new HashSet<>();
    }

    public synchronized void acceptNull() {
        totalCount++;
    }

    /**
     * 接受一个原始值；空白视为缺失；非数值仅计入总数与基数，不参与数值分布统计。
     */
    public synchronized void acceptValue(String raw) {
        totalCount++;
        if (raw == null || raw.trim().isEmpty()) {
            return;
        }
        nonNullCount++;
        String trimmed = raw.trim();
        addDistinct(trimmed);
        Double numeric = tryParse(trimmed);
        if (numeric != null) {
            // Welford 在线更新
            numericCount++;
            double delta = numeric - mean;
            mean += delta / numericCount;
            double delta2 = numeric - mean;
            m2 += delta * delta2;
        }
    }

    private void addDistinct(String value) {
        if (distinctTruncated) {
            return;
        }
        if (distinct.size() < distinctCapacity) {
            distinct.add(value);
        } else {
            distinctTruncated = true;
        }
    }

    private static Double tryParse(String raw) {
        try {
            double v = Double.parseDouble(raw);
            return Double.isNaN(v) || Double.isInfinite(v) ? null : v;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public synchronized BaselineStats snapshot() {
        double variance = numericCount > 1 ? m2 / (numericCount - 1) : 0.0;
        double stdDev = Math.sqrt(Math.max(0.0, variance));
        return new BaselineStats(totalCount, nonNullCount, numericCount,
                numericCount == 0 ? 0.0 : mean, stdDev, distinct.size());
    }
}
