package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;
import com.github.lechandonga.dataqualitypipelinejava.rule.RowRule;

import java.util.List;

/** 测试用：每条记录评估固定耗时，用于制造"摄入快于消费"的容量边界场景。 */
final class SlowRule implements RowRule {

    private final String id;
    private final long sleepMillis;

    SlowRule(String id, long sleepMillis) {
        this.id = id;
        this.sleepMillis = sleepMillis;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public List<FailureReason> evaluate(DataRecord record) {
        try {
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return List.of();
    }
}
