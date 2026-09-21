package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 引擎层基线：首次零误报、稳定批次累积、漂移批次告警并隔离漂移记录。
 */
class BaselinePipelineIntegrationTest {

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        p.setMaxRecordsPerBatch(1000);
        p.setBaselineFields("amount");
        p.setDriftZThreshold(3.0);
        p.setNullRatioDelta(0.15);
        return p;
    }

    private DefaultQualityPipeline pipeline(PipelineProperties p,
                                            InMemoryBaselineStore store,
                                            RuleSetManager manager) {
        return new DefaultQualityPipeline(p, manager, store,
                new InMemoryQuarantineService());
    }

    private RuleSetManager rules() {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(
                new RuleDefinition("nn", RuleType.NOT_NULL, "name", Map.of(), false)));
        return manager;
    }

    private BatchResult run(DefaultQualityPipeline pipeline, String id,
                            int n, int base, int spread, int nulls) {
        return pipeline.execute(id, IntStream.range(0, n).mapToObj(i -> {
            if (i < nulls) {
                return Map.of("id", String.valueOf(i), "name", "n" + i);
            }
            int value = base + ((i % 3) - 1) * spread;
            return Map.of("id", String.valueOf(i), "name", "n" + i,
                    "amount", String.valueOf(value));
        }));
    }

    @Test
    void firstRunCreatesBaselineWithoutDriftAlarms() {
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = pipeline(props(), store, rules());

        BatchResult r = run(pipeline, "first", 30, 100, 1, 0);
        assertEquals(BatchStatus.COMMITTED, r.status());
        assertEquals(0, r.failedCount(), "首次运行无历史基线，必须零误报");
        Baseline b = store.load("amount").orElseThrow();
        assertEquals(1, b.batchCount());
    }

    @Test
    void stableBatchesAccumulateBaseline() {
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = pipeline(props(), store, rules());

        run(pipeline, "b1", 30, 100, 1, 0);
        run(pipeline, "b2", 30, 100, 1, 0);
        run(pipeline, "b3", 30, 100, 1, 0);

        Baseline b = store.load("amount").orElseThrow();
        assertEquals(3, b.batchCount());
        assertEquals(90, b.totalCount());
        assertEquals(100.0, b.mean(), 1e-9);
    }

    @Test
    void meanShiftBatchRaisesExplainableAlarm() {
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = pipeline(props(), store, rules());

        // 稳定历史：99~101
        run(pipeline, "b1", 30, 100, 1, 0);
        run(pipeline, "b2", 30, 100, 1, 0);
        // 突变批次：约 200
        BatchResult shifted = run(pipeline, "b3", 30, 200, 1, 0);

        assertEquals(BatchStatus.COMMITTED, shifted.status());
        boolean drift = shifted.rejectedRecords().stream()
                .flatMap(rr -> rr.failures().stream())
                .anyMatch(f -> f.ruleId().equals("BASELINE_MEAN_SHIFT")
                        && f.field().equals("amount"));
        assertTrue(drift, "均值突变必须产生可定位到字段的漂移告警");
    }

    @Test
    void nullRatioSpikeBatchRaisesAlarm() {
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = pipeline(props(), store, rules());

        run(pipeline, "b1", 40, 100, 1, 0);
        // 50% 缺失，超过 0.15 阈值
        BatchResult shifted = run(pipeline, "b2", 40, 100, 1, 20);

        boolean alarm = shifted.rejectedRecords().stream()
                .flatMap(rr -> rr.failures().stream())
                .anyMatch(f -> f.ruleId().equals("BASELINE_NULL_RATIO"));
        assertTrue(alarm);
    }

    @Test
    void rejectedBatchDoesNotAdvanceBaseline() {
        PipelineProperties p = props();
        p.setMaxRecordsPerBatch(10);
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = pipeline(p, store, rules());

        run(pipeline, "b1", 10, 100, 1, 0);
        Baseline before = store.load("amount").orElseThrow();

        assertThrows(com.github.lechandonga.dataqualitypipelinejava.engine.BatchRejectedException.class,
                () -> run(pipeline, "too-big", 50, 999, 1, 0));

        Baseline after = store.load("amount").orElseThrow();
        assertEquals(before, after, "被拒绝批次不得推进/污染基线");
        assertEquals(1, after.batchCount());
    }
}
