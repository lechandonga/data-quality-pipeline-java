package com.github.lechandonga.dataqualitypipelinejava.archive;

import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 一个批次在执行当时的完整快照：最终结论 + 每条记录的原始内容与判定依据
 * + 重放所需的执行参数（规则版本随 {@link BatchResult#rulesVersion()} 保存）。
 * 快照本身不可变；只要快照与规则版本不变，就可以确定性地重建批次全部状态。
 */
public record BatchSnapshot(
        BatchResult result,
        List<ArchivedRecord> records,
        int workerThreads,
        int queueCapacity,
        int distinctCapacity,
        PipelineProperties replayProps,
        Set<String> repairedRecordIds
) {

    public BatchSnapshot {
        records = List.copyOf(records);
        repairedRecordIds = repairedRecordIds == null
                ? Set.of() : new LinkedHashSet<>(repairedRecordIds);
    }

    public String batchId() {
        return result.batchId();
    }
}
