package com.github.lechandonga.dataqualitypipelinejava.model;

import java.util.List;

/**
 * 单条记录的校验结论。
 */
public record RuleViolation(String recordId, RecordStatus status, List<FailureReason> reasons) {

    public boolean passed() {
        return status == RecordStatus.PASSED;
    }
}
