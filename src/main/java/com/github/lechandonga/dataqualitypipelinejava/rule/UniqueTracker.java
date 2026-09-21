package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.engine.CapacityExceededException;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 批次内唯一性跟踪器。每批次一个实例，容量有界：
 * 已观察的不同 (规则,值) 数量达到上限后拒绝继续，防止大输入无界占用内存。
 */
public final class UniqueTracker {

    private final int capacity;
    private final Map<String, Set<String>> observed = new HashMap<>();
    private int distinctCount;

    public UniqueTracker(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("uniqueTrackerCapacity 必须为正数: " + capacity);
        }
        this.capacity = capacity;
    }

    /**
     * 观察一个值。首次出现返回 true 并占用一个容量槽；重复出现返回 false。
     *
     * @throws CapacityExceededException 新值出现但容量已满
     */
    public synchronized boolean observe(RuleDefinition rule, String value) {
        String key = rule.id().trim();
        Set<String> values = observed.computeIfAbsent(key, k -> new HashSet<>());
        if (values.add(value)) {
            distinctCount++;
            if (distinctCount > capacity) {
                throw new CapacityExceededException(
                        "唯一性跟踪容量超限(" + capacity + ")，规则 " + key
                                + " 出现新值，按策略拒绝批次");
            }
            return true;
        }
        return false;
    }

    public synchronized int distinctCount() {
        return distinctCount;
    }
}
