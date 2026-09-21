package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;

/**
 * 一个批次的校验汇总结果。
 *
 * @param batchId          批次标识
 * @param ruleSetVersion   使用的规则集版本快照
 * @param status           批次状态
 * @param totalCount       本次执行处理的记录数（重放时只计本次重放条数）
 * @param passedCount      本次执行通过数
 * @param failedCount      本次执行仍失败数
 * @param cumulativePassed 批次生命周期内累计通过的不同记录数（重放不重复计数）
 * @param rejectReason     批次被拒绝的原因码（未拒绝为 null）
 * @param rejectedRecords  被隔离记录及其规则/字段级原因
 */
public record BatchResult(
        String batchId,
        String ruleSetVersion,
        BatchStatus status,
        long totalCount,
        long passedCount,
        long failedCount,
        long cumulativePassed,
        String rejectReason,
        List<RecordResult> rejectedRecords) {

    public boolean committed() {
        return status == BatchStatus.COMMITTED;
    }
}
