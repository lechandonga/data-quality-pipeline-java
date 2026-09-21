package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 将当前批次统计与历史基线对比，发现分布漂移或异常突变。
 *
 * <p>确定性语义（不得误报）：</p>
 * <ul>
 *   <li>历史基线缺失（首次运行）：返回空列表，不产生任何告警；</li>
 *   <li>当前批次或历史样本量不足：跳过对应检查；</li>
 *   <li>历史标准差为 0（常量分布）：仅当当前均值与历史均值确实不同才告警；</li>
 *   <li>数值漂移：当前均值相对历史均值偏离超过 zThreshold 个历史标准差；</li>
 *   <li>缺失率漂移：当前缺失率与历史缺失率之差超过 nullRatioDelta。</li>
 * </ul>
 */
public final class BaselineChecker {

    private final double driftZThreshold;
    private final double nullRatioDelta;
    private final long minSampleSize;

    public BaselineChecker(double driftZThreshold, double nullRatioDelta) {
        this(driftZThreshold, nullRatioDelta, 2L);
    }

    public BaselineChecker(double driftZThreshold, double nullRatioDelta, long minSampleSize) {
        if (driftZThreshold <= 0 || nullRatioDelta < 0 || minSampleSize <= 0) {
            throw new IllegalArgumentException(
                    "driftZThreshold 必须为正，nullRatioDelta 不能为负，minSampleSize 必须为正");
        }
        this.driftZThreshold = driftZThreshold;
        this.nullRatioDelta = nullRatioDelta;
        this.minSampleSize = minSampleSize;
    }

    public List<ValidationFailure> check(String field,
                                         BaselineStats current,
                                         Optional<BaselineStats> historical) {
        if (historical == null || historical.isEmpty()) {
            // 首次运行 / 基线缺失：确定地不告警
            return List.of();
        }
        BaselineStats base = historical.get();
        List<ValidationFailure> failures = new ArrayList<>();

        double currentNullRatio = current.nullRatio();
        double baseNullRatio = base.nullRatio();
        if (current.count() >= minSampleSize && base.count() >= minSampleSize
                && Math.abs(currentNullRatio - baseNullRatio) > nullRatioDelta) {
            failures.add(new ValidationFailure(
                    "BASELINE_NULL_RATIO",
                    "BASELINE",
                    field,
                    String.format(
                            "缺失率突变: 当前=%.4f, 历史基线=%.4f, 允许偏差<=%.4f",
                            currentNullRatio, baseNullRatio, nullRatioDelta)));
        }

        if (current.nonNullCount() >= minSampleSize && base.nonNullCount() >= minSampleSize
                && base.stdDev() > 0.0) {
            double z = Math.abs(current.mean() - base.mean()) / base.stdDev();
            if (z > driftZThreshold) {
                failures.add(new ValidationFailure(
                        "BASELINE_MEAN_SHIFT",
                        "BASELINE",
                        field,
                        String.format(
                                "均值漂移: 当前=%.6f, 历史均值=%.6f, 历史标准差=%.6f, z=%.3f>%.3f",
                                current.mean(), base.mean(), base.stdDev(),
                                z, driftZThreshold)));
            }
        } else if (current.nonNullCount() >= minSampleSize && base.nonNullCount() >= minSampleSize
                && base.stdDev() == 0.0 && current.mean() != base.mean()) {
            failures.add(new ValidationFailure(
                    "BASELINE_CONSTANT_BREAK",
                    "BASELINE",
                    field,
                    String.format(
                            "历史为常量分布(%.6f)，当前均值变为 %.6f",
                            base.mean(), current.mean())));
        }
        return failures;
    }
}
