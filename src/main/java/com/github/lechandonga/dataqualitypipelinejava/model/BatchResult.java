package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;

/**
 * 一个批次的完整校验结论。violations 按输入顺序排列，保证可复现；
 * replay 只追加/更新隔离记录的结论，已通过记录结论永不变更。
 */
public record BatchResult(
        String batchId,
        String dataset,
        BatchStatus status,
        String rulesVersion,
        long total,
        long passedCount,
        long quarantinedCount,
        long overflowIsolatedCount,
        List<RuleViolation> violations,
        List<DriftFinding> driftFindings,
        boolean baselineUpdated,
        String rejectReason
) {

    public boolean committed() {
        return status == BatchStatus.COMMITTED;
    }
}
