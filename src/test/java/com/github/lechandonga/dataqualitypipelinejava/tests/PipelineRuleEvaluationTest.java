package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.CompositeMode;
import com.github.lechandonga.dataqualitypipelinejava.model.FailureReason;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleViolation;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.cross;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.regex;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineRuleEvaluationTest {

    private PipelineService pipeline;
    private RuleRegistry registry;
    private InMemoryBaselineStore store;

    private PipelineProperties props() {
        return new PipelineProperties(4, 8, 100, 10_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null);
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryBaselineStore();
        registry = new RuleRegistry(new RuleCompiler());
        pipeline = new PipelineService(registry, new BaselineService(store), props());
    }

    private BatchResult run(String batchId, com.github.lechandonga.dataqualitypipelinejava.model.DataRecord... records) {
        return pipeline.execute(ExecutionRequest.of(batchId, "ds", false), List.of(records).iterator());
    }

    @Test
    void notNullRuleQuarantinesBlankWithLocatableReason() {
        registry.replaceAll(List.of(notNull("nn-name", "name")));
        BatchResult r = run("b1",
                rec("ok", Map.of("name", "alice")),
                rec("bad", Map.of("name", "  ")),
                rec("missing", Map.of()));
        assertEquals(3, r.total());
        assertEquals(1, r.passedCount());
        assertEquals(2, r.quarantinedCount());
        RuleViolation v = r.violations().get(1);
        assertEquals("bad", v.recordId());
        FailureReason reason = v.reasons().get(0);
        assertEquals("nn-name", reason.ruleId());
        assertEquals("name", reason.field());
    }

    @Test
    void rangeRuleChecksNumericBoundsAndRejectsNonNumeric() {
        registry.replaceAll(List.of(range("r-age", "age", "0", "120")));
        BatchResult r = run("b1",
                rec("a", Map.of("age", "30")),
                rec("b", Map.of("age", "200")),
                rec("c", Map.of("age", "x")),
                rec("d", Map.of("age", "-1")));
        assertEquals(1, r.passedCount());
        assertEquals(3, r.quarantinedCount());
        assertEquals("r-age", r.violations().get(2).reasons().get(0).ruleId());
    }

    @Test
    void regexRuleMatchesPattern() {
        registry.replaceAll(List.of(regex("rx-mail", "email", "\\w+@\\w+\\.\\w+")));
        BatchResult r = run("b1",
                rec("ok", Map.of("email", "a@b.cd")),
                rec("bad", Map.of("email", "nope")));
        assertEquals(RecordStatus.PASSED, r.violations().get(0).status());
        assertEquals("rx-mail", r.violations().get(1).reasons().get(0).ruleId());
        assertEquals("email", r.violations().get(1).reasons().get(0).field());
    }

    @Test
    void crossFieldRuleEnforcesConsistency() {
        registry.replaceAll(List.of(cross("x-le", "end", "start", "GE")));
        BatchResult r = run("b1",
                rec("ok", Map.of("start", "1", "end", "2")),
                rec("bad", Map.of("start", "9", "end", "1")));
        assertEquals(1, r.passedCount());
        FailureReason reason = r.violations().get(1).reasons().get(0);
        assertEquals("x-le", reason.ruleId());
        assertEquals("end,start", reason.field());
    }

    @Test
    void compositeRuleReusesChildrenWithAndSemantics() {
        registry.replaceAll(List.of(
                notNull("nn-a", "a"),
                range("r-b", "b", "0", "10"),
                com.github.lechandonga.dataqualitypipelinejava.tests.TestData
                        .composite("c1", CompositeMode.AND, List.of("nn-a", "r-b"))));
        BatchResult r = run("b1",
                rec("ok", Map.of("a", "x", "b", "5")),
                rec("bad", Map.of("a", "x", "b", "99")));
        // 复合规则失败，且其引用的子规则也独立报告（规则可复用、原因可解释）
        RuleViolation bad = r.violations().get(1);
        assertTrue(bad.reasons().stream().anyMatch(x -> x.ruleId().equals("c1")));
        assertTrue(bad.reasons().stream().anyMatch(x -> x.ruleId().equals("r-b")));
    }

    @Test
    void uniqueRuleMarksAllDuplicatesAfterFirstOccurrence() {
        registry.replaceAll(List.of(unique("u-email", List.of("email"))));
        BatchResult r = run("b1",
                rec("r1", Map.of("email", "a@x")),
                rec("r2", Map.of("email", "b@x")),
                rec("r3", Map.of("email", "a@x")),
                rec("r4", Map.of("email", "a@x")));
        assertEquals(2, r.passedCount());
        assertEquals(2, r.quarantinedCount());
        FailureReason reason = r.violations().get(2).reasons().get(0);
        assertEquals("u-email", reason.ruleId());
        assertTrue(reason.expected().contains("r1"), "must point to the first record: " + reason);
        assertFalse(r.violations().get(0).reasons().stream()
                .anyMatch(x -> x.ruleId().equals("u-email")));
    }
}
