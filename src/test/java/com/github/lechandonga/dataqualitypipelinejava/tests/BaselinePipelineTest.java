package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DriftStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BaselinePipelineTest {

    private InMemoryBaselineStore store;
    private PipelineService pipeline;
    private RuleRegistry registry;

    private PipelineProperties props(double meanThreshold, double nullThreshold) {
        return new PipelineProperties(4, 8, 100, 10_000,
                OverflowPolicy.ISOLATE, meanThreshold, nullThreshold, null, null, 100);
    }

    @BeforeEach
    void setUp() {
        store = new InMemoryBaselineStore();
        registry = new RuleRegistry(new RuleCompiler());
        pipeline = PipelineService.withoutArchive(registry, new BaselineService(store), props(3.0, 0.3));
    }

    private BatchResult execute(String batchId, String dataset, boolean commit,
                                com.github.lechandonga.dataqualitypipelinejava.model.DataRecord... recs) {
        return pipeline.execute(new ExecutionRequest(batchId, dataset, commit, null, null),
                List.of(recs).iterator());
    }

    @Test
    void firstRunReportsNoBaselineAndNeverDrifted() {
        BatchResult r = execute("b1", "ds", false,
                rec("1", Map.of("v", "10")), rec("2", Map.of("v", "11")));
        assertTrue(r.driftFindings().stream().allMatch(d -> d.status() == DriftStatus.NO_BASELINE));
        assertTrue(r.driftFindings().stream().noneMatch(d -> d.status() == DriftStatus.DRIFTED));
        assertEquals(BatchStatus.ACCEPTED, r.status());
        assertFalse(r.baselineUpdated());
        assertTrue(store.load("ds").isEmpty(), "dry-run must not touch baseline");
    }

    @Test
    void dryRunDoesNotUpdateBaseline() {
        execute("b1", "ds", false, rec("1", Map.of("v", "1")));
        assertTrue(store.load("ds").isEmpty());
    }

    @Test
    void onlyCommittedBatchUpdatesBaseline() {
        execute("b1", "ds", true,
                rec("1", Map.of("v", "10")), rec("2", Map.of("v", "12")));
        Optional<Baseline> b = store.load("ds");
        assertTrue(b.isPresent());
        assertEquals(1, b.get().batchCount());
        assertEquals(11.0, b.get().fieldStats().get("v").mean(), 1e-9);
        assertEquals(2, b.get().fieldStats().get("v").count());
    }

    @Test
    void stableSecondBatchShowsNoDrift() {
        execute("b1", "ds", true,
                rec("1", Map.of("v", "100")), rec("2", Map.of("v", "102")),
                rec("3", Map.of("v", "99")), rec("4", Map.of("v", "101")));
        BatchResult r2 = execute("b2", "ds", false,
                rec("5", Map.of("v", "100")), rec("6", Map.of("v", "101")));
        assertTrue(r2.driftFindings().stream().noneMatch(d -> d.status() == DriftStatus.DRIFTED),
                "near-identical batch must not be flagged: " + r2.driftFindings());
    }

    @Test
    void meanShiftBeyondThresholdIsDrift() {
        // 基线：均值100、标准差很小
        execute("b1", "ds", true,
                rec("1", Map.of("v", "100")), rec("2", Map.of("v", "100")),
                rec("3", Map.of("v", "101")), rec("4", Map.of("v", "99")));
        // 批次：均值突变
        BatchResult r2 = execute("b2", "ds", false,
                rec("5", Map.of("v", "130")), rec("6", Map.of("v", "131")));
        assertTrue(r2.driftFindings().stream()
                .anyMatch(d -> d.metric().equals("mean-zscore") && d.status() == DriftStatus.DRIFTED),
                "mean jump must be detected: " + r2.driftFindings());
    }

    @Test
    void nullRateShiftIsDetected() {
        // 基线：v 全部非空
        execute("b1", "ds", true,
                rec("1", Map.of("v", "1")), rec("2", Map.of("v", "2")));
        // 批次：v 全部缺失（字段不存在），缺失率 0 -> 1
        BatchResult r2 = execute("b2", "ds", false,
                rec("3", Map.of("other", "1")), rec("4", Map.of("other", "2")));
        assertTrue(r2.driftFindings().stream()
                .anyMatch(d -> d.field().equals("v")
                        && d.metric().equals("null-rate")
                        && d.status() == DriftStatus.DRIFTED),
                "null-rate jump on the SAME field must be detected: " + r2.driftFindings());
    }

    @Test
    void failedBaselineWriteKeepsOldBaselineAndBatchNotCommitted() {
        execute("b1", "ds", true, rec("1", Map.of("v", "10")));
        Baseline before = store.load("ds").orElseThrow();

        store.armFailure(); // 下一次保存必失败
        BatchResult r2 = execute("b2", "ds", true, rec("2", Map.of("v", "999")));

        assertFalse(r2.baselineUpdated(), "commit flag must be false on storage failure");
        assertEquals(BatchStatus.ACCEPTED, r2.status(), "batch must not be marked COMMITTED");
        Baseline after = store.load("ds").orElseThrow();
        assertEquals(before.batchCount(), after.batchCount(), "baseline must stay untouched");
        assertEquals(before.fieldStats().get("v").mean(), after.fieldStats().get("v").mean(), 0.0);
    }

    @Test
    void rejectedBatchNeverPollutesBaseline() {
        // REJECT 策略 + 极小唯一键容量：超过容量的全新键无法确认唯一，整批拒绝
        PipelineProperties rejectProps = new PipelineProperties(4, 8, 1_000, 2,
                OverflowPolicy.REJECT, 3.0, 0.3, null, null, 100);
        PipelineService rejecting = PipelineService.withoutArchive(registry, new BaselineService(store), rejectProps);
        registry.replaceAll(List.of(
                com.github.lechandonga.dataqualitypipelinejava.tests.TestData
                        .unique("u-v", List.of("v"))));

        BatchResult r = rejecting.execute(new ExecutionRequest("bx", "ds2", true, 4, null),
                List.of(
                        rec("1", Map.of("v", "a")),
                        rec("2", Map.of("v", "b")),
                        rec("3", Map.of("v", "c"))).iterator());

        assertEquals(BatchStatus.REJECTED, r.status());
        assertFalse(r.baselineUpdated());
        assertTrue(store.load("ds2").isEmpty(), "rejected batch must not create baseline");
    }

    @Test
    void baselineAccumulatesAcrossCommits() {
        execute("b1", "ds", true, rec("1", Map.of("v", "10")));
        execute("b2", "ds", true, rec("2", Map.of("v", "20")));
        execute("b3", "ds", true, rec("3", Map.of("v", "30")));
        Baseline b = store.load("ds").orElseThrow();
        assertEquals(3, b.batchCount());
        assertEquals(3, b.fieldStats().get("v").count());
        assertEquals(20.0, b.fieldStats().get("v").mean(), 1e-9);
    }
}
