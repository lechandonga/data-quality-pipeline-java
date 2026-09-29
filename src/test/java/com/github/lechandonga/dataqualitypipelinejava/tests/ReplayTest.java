package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordStatus;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayOutcome;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReplayTest {

    private PipelineService pipeline;
    private RuleRegistry registry;
    private InMemoryBaselineStore store;

    private PipelineProperties props() {
        return new PipelineProperties(4, 8, 256, 1_000_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null, null, 100);
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryBaselineStore();
        registry = new RuleRegistry(new RuleCompiler());
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "100"),
                unique("u-id", List.of("id"))));
        pipeline = new PipelineService(registry, new BaselineService(store), props());
    }

    private DataRecord recV(long order, String recId, String id, String v) {
        Map<String, String> d = new LinkedHashMap<>();
        d.put("id", id);
        if (v != null) {
            d.put("v", v);
        }
        return rec(order, recId, d);
    }

    @Test
    void replayRepairedRecordsIsIdempotentAndDoesNotDoubleCount() {
        BatchResult first = pipeline.execute(ExecutionRequest.of("b1", "ds", false), List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "999"),   // 超范围
                recV(2, "rec-2", "id-2", null),    // 缺失
                recV(3, "rec-3", "id-0", "10")).iterator()); // 唯一冲突
        assertEquals(4, first.total());
        assertEquals(1, first.passedCount());
        assertEquals(3, first.quarantinedCount());

        // 修复 rec-1 与 rec-2（rec-3 仍与 rec-0 冲突，保持隔离）
        BatchResult replayed = pipeline.replay("b1", List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20")).iterator()).batch();

        assertEquals(4, replayed.total(), "replay must not duplicate records");
        assertEquals(3, replayed.passedCount());
        assertEquals(1, replayed.quarantinedCount());

        // 再次同样重放，结论不变（幂等）
        BatchResult again = pipeline.replay("b1", List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20")).iterator()).batch();
        assertEquals(replayed.passedCount(), again.passedCount());
        assertEquals(replayed.quarantinedCount(), again.quarantinedCount());
        assertEquals(
                replayed.violations().stream().map(v -> v.recordId() + v.status()).toList(),
                again.violations().stream().map(v -> v.recordId() + v.status()).toList());
    }

    @Test
    void replayNeverChangesAlreadyPassedRecords() {
        BatchResult first = pipeline.execute(ExecutionRequest.of("b1", "ds", false), List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "999")).iterator());

        BatchResult replayed = pipeline.replay("b1", List.of(
                recV(1, "rec-1", "id-1", "50")).iterator()).batch();

        // rec-0 结论与输入顺序位置不变
        assertEquals("rec-0", replayed.violations().get(0).recordId());
        assertEquals(RecordStatus.PASSED, replayed.violations().get(0).status());
        assertTrue(replayed.violations().get(0).reasons().isEmpty());
    }

    @Test
    void replayRejectsWhenRulesVersionChanged() {
        pipeline.execute(ExecutionRequest.of("b1", "ds", false), List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "999")).iterator());

        // 规则演进
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "10"),
                unique("u-id", List.of("id"))));

        ReplayRejectedException ex = assertThrows(ReplayRejectedException.class,
                () -> pipeline.replay("b1", List.of(recV(1, "rec-1", "id-1", "5")).iterator()),
                "replay under a different rules version must be refused (reproducibility)");
        assertEquals("RULES_VERSION_MISMATCH", ex.reason());
    }

    @Test
    void replayDoesNotUpdateBaselineAgain() {
        BatchResult committed = pipeline.execute(
                new ExecutionRequest("b1", "ds", true, null, null), List.of(
                        recV(0, "rec-0", "id-0", "10"),
                        recV(1, "rec-1", "id-1", "999")).iterator());
        assertEquals(BatchStatus.COMMITTED, committed.status());
        long batchesAfterCommit = store.load("ds").orElseThrow().batchCount();

        BatchResult replayed = pipeline.replay("b1",
                List.of(recV(1, "rec-1", "id-1", "50")).iterator()).batch();
        assertTrue(replayed.baselineUpdated(), "replay keeps original committed flag");
        assertEquals(batchesAfterCommit, store.load("ds").orElseThrow().batchCount(),
                "replay must not add another baseline revision");
    }

    @Test
    void replayUnknownBatchFails() {
        ReplayRejectedException unknown = assertThrows(ReplayRejectedException.class,
                () -> pipeline.replay("nope", List.of(recV(0, "x", "id", "1")).iterator()));
        assertEquals("BATCH_NOT_FOUND", unknown.reason());
    }

    @Test
    void quarantinedReasonsRemainLocatableAfterReplay() {
        pipeline.execute(ExecutionRequest.of("b1", "ds", false), List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-0", "10")).iterator());
        ReplayOutcome outcome = pipeline.replay("b1",
                List.of(recV(1, "rec-1", "id-0", "10")).iterator());
        BatchResult r = outcome.batch();
        var stillBad = r.violations().get(1);
        assertEquals(RecordStatus.QUARANTINED, stillBad.status());
        assertEquals("u-id", stillBad.reasons().get(0).ruleId());
        assertEquals("id", stillBad.reasons().get(0).field());
        assertFalse(stillBad.reasons().get(0).expected().isBlank());
        assertEquals(List.of("rec-1"), outcome.stillQuarantined());
        assertTrue(outcome.nowPassed().isEmpty());
    }
}
