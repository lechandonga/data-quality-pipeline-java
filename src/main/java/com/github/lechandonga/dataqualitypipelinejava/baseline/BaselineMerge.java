package com.github.lechandonga.dataqualitypipelinejava.baseline;

import java.util.Optional;

/**
 * 基线合并：把一次成功批次的统计与历史基线按样本量加权合并。
 * 合并为纯函数，结果只依赖双方统计，保证可复现。
 */
public final class BaselineMerge {

    private BaselineMerge() {
    }

    public static Baseline merge(String field, Optional<Baseline> historical, BaselineStats current) {
        double currentM2 = current.numericCount() > 1
                ? current.stdDev() * current.stdDev() * (current.numericCount() - 1)
                : 0.0;
        if (historical.isEmpty()) {
            // 首次提交：当前批次统计即基线
            return new Baseline(field, 1,
                    current.count(), current.nonNullCount(), current.numericCount(),
                    current.mean(), currentM2,
                    current.distinctCount());
        }
        Baseline h = historical.get();
        long na = h.numericCount();
        long nb = current.numericCount();
        double mean;
        double m2;
        if (na + nb == 0) {
            mean = 0.0;
            m2 = 0.0;
        } else if (na == 0) {
            mean = current.mean();
            m2 = currentM2;
        } else if (nb == 0) {
            mean = h.mean();
            m2 = h.m2();
        } else {
            // Chan 并行合并 Welford 统计量
            long total = na + nb;
            double delta = current.mean() - h.mean();
            mean = h.mean() + delta * nb / total;
            m2 = h.m2() + currentM2 + delta * delta * na * nb / total;
        }
        return new Baseline(field,
                h.batchCount() + 1,
                h.totalCount() + current.count(),
                h.nonNullCount() + current.nonNullCount(),
                na + nb,
                mean,
                m2,
                Math.max(h.distinctCount(), current.distinctCount()));
    }
}
