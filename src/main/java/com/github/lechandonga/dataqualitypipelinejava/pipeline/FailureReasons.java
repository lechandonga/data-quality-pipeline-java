package com.github.lechandonga.dataqualitypipelinejava.pipeline;

import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;

/** 流水线内部产生的失败原因（容量隔离/拒绝、唯一性冲突）。 */
final class FailureReasons {

    static final String CAPACITY_RULE = "__capacity__";
    static final String UNIQUE_RULE_PREFIX = "";

    private FailureReasons() {
    }

    static FailureReason capacity(String capacityName, long limit) {
        return new FailureReason(CAPACITY_RULE, capacityName, "capacity limit reached",
                capacityName + " <= " + limit + " (overflow isolate)");
    }

    static FailureReason unique(String ruleId, String fields, String key, String firstRecordId) {
        return new FailureReason(ruleId, fields, key,
                "unique value first seen by record=" + firstRecordId);
    }
}
