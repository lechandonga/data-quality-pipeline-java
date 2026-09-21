package com.github.lechandonga.dataqualitypipelinejava.engine;

import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleSet;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * 本地数据质量校验流水线入口（骨架）。
 */
public interface QualityPipeline {

    /**
     * 使用当前已发布规则集执行批次。
     */
    BatchResult execute(String batchId, Stream<Map<String, String>> records);

    /**
     * 使用指定规则集快照执行批次（并发场景下保证该批次结论不受规则更新影响）。
     */
    BatchResult execute(String batchId, RuleSet ruleSet, Stream<Map<String, String>> records);

    /**
     * 重放指定批次中此前被隔离的记录；已通过记录结论不变、不重复计数。
     */
    BatchResult replay(String batchId, List<QuarantineService.ReplayInput> fixedRecords);
}
