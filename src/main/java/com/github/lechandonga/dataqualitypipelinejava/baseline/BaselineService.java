package com.github.lechandonga.dataqualitypipelinejava.baseline;

import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftFinding;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.FieldStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基线领域服务：漂移检测、基线合并与"仅成功提交后更新"的原子提交。
 *
 * 语义约定：
 * 1. 首次运行或基线缺失：所有字段返回 NO_BASELINE，绝不产生 DRIFTED 误报；
 * 2. 漂移指标包括数值均值（z-score）与缺失率（绝对差），口径固定；
 * 3. 合并基于独立批次矩（Chan 并行方差合并公式），与批次大小无关；
 * 4. 只有批次成功提交路径才会调用 commit；store 原子保存失败则整体失败，旧基线不变。
 */
@Service
public class BaselineService {

    private static final Logger log = LoggerFactory.getLogger(BaselineService.class);

    private final BaselineStore store;

    public BaselineService(BaselineStore store) {
        this.store = store;
    }

    public Optional<Baseline> current(String dataset) {
        return store.load(dataset);
    }

    /**
     * 比较当前批次统计与历史基线。基线缺失时对每个字段给出 NO_BASELINE。
     */
    public List<DriftFinding> checkDrift(String dataset, Map<String, FieldStats> batchStats,
                                         double meanShiftThreshold, double nullRateThreshold) {
        Optional<Baseline> existing = store.load(dataset);
        if (existing.isEmpty() || existing.get().batchCount() == 0) {
            log.info("no baseline for dataset={}, batch treated as first run (no false positives)", dataset);
            List<DriftFinding> findings = new ArrayList<>();
            for (String field : batchStats.keySet()) {
                findings.add(new DriftFinding(field, DriftStatus.NO_BASELINE, "first-run",
                        0.0, 0.0, "baseline absent; nothing to compare"));
            }
            return findings;
        }
        Baseline base = existing.get();
        List<DriftFinding> findings = new ArrayList<>();
        // 批次中出现的字段
        for (Map.Entry<String, FieldStats> e : batchStats.entrySet()) {
            FieldStats bStats = e.getValue();
            FieldStats aStats = base.fieldStats().get(e.getKey());
            if (aStats == null) {
                findings.add(new DriftFinding(e.getKey(), DriftStatus.NO_BASELINE, "new-field",
                        0.0, 0.0, "field absent from baseline"));
                continue;
            }
            findings.addAll(compareField(e.getKey(), aStats, bStats,
                    meanShiftThreshold, nullRateThreshold));
        }
        // 基线有、本批次完全缺失的字段：按 100% 缺失参与比较（仍不产生"无基线误报"）
        for (Map.Entry<String, FieldStats> e : base.fieldStats().entrySet()) {
            if (!batchStats.containsKey(e.getKey())) {
                findings.addAll(compareField(e.getKey(), e.getValue(), FieldStats.EMPTY,
                        meanShiftThreshold, nullRateThreshold));
            }
        }
        return findings;
    }

    private List<DriftFinding> compareField(String field, FieldStats base, FieldStats batch,
                                            double meanShiftThreshold, double nullRateThreshold) {
        List<DriftFinding> out = new ArrayList<>();

        // 均值漂移：|batchMean - baseMean| / baseStd > 阈值
        double baseStd = base.stdDev();
        if (baseStd > 0.0) {
            double z = Math.abs(batch.mean() - base.mean()) / baseStd;
            DriftStatus s = z > meanShiftThreshold ? DriftStatus.DRIFTED : DriftStatus.OK;
            out.add(new DriftFinding(field, s, "mean-zscore",
                    base.mean(), batch.mean(),
                    "|z|=" + round(z) + " threshold=" + meanShiftThreshold));
        }

        // 缺失率漂移：绝对差 > 阈值
        double baseNullRate = base.count() == 0 ? 0.0 : (double) base.nullCount() / base.count();
        double batchNullRate = batch.count() == 0 ? 0.0 : (double) batch.nullCount() / batch.count();
        double diff = Math.abs(batchNullRate - baseNullRate);
        DriftStatus s = diff > nullRateThreshold ? DriftStatus.DRIFTED : DriftStatus.OK;
        out.add(new DriftFinding(field, s, "null-rate",
                round(baseNullRate), round(batchNullRate),
                "|delta|=" + round(diff) + " threshold=" + nullRateThreshold));
        return out;
    }

    /**
     * 仅在批次成功提交路径调用：把本批次统计合并进历史基线并原子落盘。
     */
    public Baseline commit(String dataset, Map<String, FieldStats> batchStats) {
        Baseline previous = store.load(dataset).orElseGet(() -> Baseline.empty(dataset));
        Map<String, FieldStats> merged = new LinkedHashMap<>(previous.fieldStats());
        for (Map.Entry<String, FieldStats> e : batchStats.entrySet()) {
            merged.merge(e.getKey(), e.getValue(),
                    (a, b) -> mergeStats(previous, e.getKey(), a, b));
        }
        // 对新增字段，直接采用批次统计；对既有字段 merge 已处理
        Baseline updated = new Baseline(dataset, previous.batchCount() + 1, merged);
        try {
            store.save(dataset, updated);
        } catch (RuntimeException ex) {
            // 存储失败：不更新内存视图，交由调用方拒绝提交
            throw new BaselineCommitException(
                    "failed to atomically persist baseline for dataset=" + dataset, ex);
        }
        log.info("baseline committed: dataset={}, batchCount={}, fields={}",
                dataset, updated.batchCount(), updated.fieldStats().size());
        return updated;
    }

    private FieldStats mergeStats(Baseline previous, String field, FieldStats a, FieldStats b) {
        // a 为历史累计（nA 批），b 为当前批次；采用矩合并
        long nA = a.count();
        long nB = b.count();
        long n = nA + nB;
        if (n == 0) {
            return FieldStats.EMPTY;
        }
        double delta = b.mean() - a.mean();
        double mean = (nA * a.mean() + nB * b.mean()) / n;
        // Chan 公式（以记录数为权重；历史基线代表历史全部记录）
        double m2A = a.variance() * nA;
        double m2B = b.variance() * nB;
        double m2 = m2A + m2B + delta * delta * nA * nB / n;
        double min = Math.min(a.min(), b.min());
        double max = Math.max(a.max(), b.max());
        long distinct = Math.max(a.distinct(), b.distinct());
        return new FieldStats(n, a.nullCount() + b.nullCount(), mean, m2 / n, min, max, distinct);
    }

    private static double round(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }
}
