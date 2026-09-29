package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import java.util.List;

/**
 * 重放输入中包含不属于该批次当前隔离清单的记录。
 * 按"整体不生效"处理：不重放任何一条，已通过记录结论不变，不产生部分重放。
 * 原因码 {@code NOT_IN_QUARANTINE_LIST} 与"批次不存在"、规则版本不一致可区分。
 */
public class BatchReplayRejectedException extends RuntimeException {

    private final String batchId;
    private final List<String> rejectedRecordIds;

    public BatchReplayRejectedException(String batchId, List<String> rejectedRecordIds) {
        super("replay rejected: records not in batch quarantine list: " + rejectedRecordIds);
        this.batchId = batchId;
        this.rejectedRecordIds = List.copyOf(rejectedRecordIds);
    }

    public String batchId() {
        return batchId;
    }

    public List<String> rejectedRecordIds() {
        return rejectedRecordIds;
    }

    public String reasonCode() {
        return "NOT_IN_QUARANTINE_LIST";
    }
}
