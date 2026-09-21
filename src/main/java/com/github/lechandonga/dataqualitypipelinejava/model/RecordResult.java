package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;
import java.util.Map;

/**
 * 单条记录的校验结论。
 *
 * @param recordKey  记录定位键（主键字段或序号）
 * @param passed     是否通过
 * @param failures   失败原因（可定位规则与字段）
 * @param source     原始记录快照（仅隔离记录保留，供重放重判唯一性）
 */
public record RecordResult(
        String recordKey,
        boolean passed,
        List<ValidationFailure> failures,
        Map<String, String> source) {

    public RecordResult(String recordKey, boolean passed, List<ValidationFailure> failures) {
        this(recordKey, passed, failures, null);
    }
}
