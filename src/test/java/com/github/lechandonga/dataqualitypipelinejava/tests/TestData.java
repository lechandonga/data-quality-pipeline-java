package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.CompositeMode;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** 测试构造工具。 */
final class TestData {

    private TestData() {
    }

    static DataRecord rec(String id, Map<String, String> data) {
        return new DataRecord(id, -1, data);
    }

    static DataRecord rec(long order, String id, Map<String, String> data) {
        return new DataRecord(id, order, data);
    }

    static Iterator<DataRecord> list(DataRecord... records) {
        return List.of(records).iterator();
    }

    static Iterator<DataRecord> range(int n, java.util.function.Function<Integer, DataRecord> f) {
        return IntStream.range(0, n).mapToObj(f::apply).iterator();
    }

    static RuleDefinition notNull(String id, String field) {
        return new RuleDefinition(id, RuleType.NOT_NULL, field, null, null, null,
                null, null, null, null, null);
    }

    static RuleDefinition range(String id, String field, String min, String max) {
        return new RuleDefinition(id, RuleType.RANGE, field, min, max, null,
                null, null, null, null, null);
    }

    static RuleDefinition regex(String id, String field, String pattern) {
        return new RuleDefinition(id, RuleType.REGEX, field, null, null, pattern,
                null, null, null, null, null);
    }

    static RuleDefinition cross(String id, String field, String other, String op) {
        return new RuleDefinition(id, RuleType.CROSS_FIELD, field, null, null, null,
                other, op, null, null, null);
    }

    static RuleDefinition unique(String id, List<String> fields) {
        return new RuleDefinition(id, RuleType.UNIQUE, null, null, null, null,
                null, null, null, fields, null);
    }

    static RuleDefinition composite(String id, CompositeMode mode, List<String> children) {
        return new RuleDefinition(id, RuleType.COMPOSITE, null, null, null, null,
                null, null, mode, children, null);
    }

    static PipelineProperties props(int workers, int queueCapacity, int distinctCapacity,
                                    OverflowPolicy policy) {
        return new PipelineProperties(workers, 8, queueCapacity, distinctCapacity,
                policy, 3.0, 0.3, null, null, 100);
    }
}
