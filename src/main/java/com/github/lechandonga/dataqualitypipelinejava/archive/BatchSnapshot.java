package com.github.lechandonga.dataqualitypipelinejava.archive;

import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftFinding;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;
import com.github.lechandonga.dataqualitypipelinejava.model.PersistenceStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleViolation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * 一个批次在执行时刻的完整结论快照：批次级结论 + 每条记录的原始内容、
 * 原始顺序与判定依据。存档后可在新进程中无损重建（查询与重放）。
 *
 * <p>快照只含执行时事实（含规则版本号），不含规则定义本身：
 * 重放时仍要求当前规则版本与之一致，否则照旧拒绝。
 */
public record BatchSnapshot(
        String batchId,
        String dataset,
        BatchStatus status,
        String rulesVersion,
        int workers,
        boolean commit,
        String rejectReason,
        List<DriftFinding> driftFindings,
        boolean baselineUpdated,
        List<Entry> entries
) {

    /** 一条记录的原始顺序、原始字段内容与当时的判定结论。 */
    public record Entry(long order, String recordId, LinkedHashMap<String, String> data,
                        RecordStatus status, List<FailureReason> reasons) {
    }

    public long total() {
        return entries.size();
    }

    public long passedCount() {
        return entries.stream().filter(e -> e.status() == RecordStatus.PASSED).count();
    }

    public long quarantinedCount() {
        return entries.stream().filter(e -> e.status() == RecordStatus.QUARANTINED).count();
    }

    /** 重建对外结论；存档读出的结论固定标记为 PERSISTED。 */
    public BatchResult toResult() {
        List<RuleViolation> violations = new ArrayList<>();
        for (Entry e : entries) {
            violations.add(new RuleViolation(e.recordId(), e.status(),
                    List.copyOf(e.reasons())));
        }
        return new BatchResult(batchId, dataset, status, rulesVersion, entries.size(),
                passedCount(), quarantinedCount(), countOverflow(violations),
                List.copyOf(violations), List.copyOf(driftFindings),
                baselineUpdated, rejectReason, PersistenceStatus.PERSISTED);
    }

    /** 当前仍被隔离的记录（按原始顺序），用于导出修复文件。 */
    public List<DataRecord> quarantinedRecords() {
        List<DataRecord> out = new ArrayList<>();
        for (Entry e : entries) {
            if (e.status() == RecordStatus.QUARANTINED) {
                out.add(new DataRecord(e.recordId(), e.order(), new LinkedHashMap<>(e.data())));
            }
        }
        return out;
    }

    private static long countOverflow(List<RuleViolation> violations) {
        return violations.stream()
                .filter(v -> v.reasons().stream()
                        .anyMatch(r -> "__capacity__".equals(r.ruleId())))
                .count();
    }
}
