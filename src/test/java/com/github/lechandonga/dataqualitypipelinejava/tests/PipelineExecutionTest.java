package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.BatchRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRuleSet;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 批次执行：唯一性、容量拒绝/隔离、并发、规则快照冻结、可复现。
 */
class PipelineExecutionTest {

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        p.setMaxConcurrentBatches(4);
        p.setMaxRecordsPerBatch(100);
        p.setRecordQueueCapacity(16);
        p.setUniqueTrackerCapacity(100_000);
        p.setBaselineFields("");
        return p;
    }

    private RuleSetManager managerWith(RuleDefinition... defs) {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(defs));
        return manager;
    }

    private RuleDefinition def(String id, RuleType type, String field,
                               Map<String, String> params, boolean nullable) {
        return new RuleDefinition(id, type, field, params, nullable);
    }

    private Map<String, String> rec(int id) {
        return Map.of("id", "r" + id, "name", "name" + id, "age", String.valueOf(20 + id));
    }

    @Test
    void uniqueViolationIsolatedWithRuleAndField() {
        RuleSetManager manager = managerWith(
                def("u_name", RuleType.UNIQUE, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(props(), manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        BatchResult result = pipeline.execute("b", Stream.of(
                Map.of("id", "1", "name", "dup"),
                Map.of("id", "2", "name", "dup"),
                Map.of("id", "3", "name", "other")));

        assertEquals(BatchStatus.COMMITTED, result.status());
        assertEquals(2, result.passedCount());
        assertEquals(1, result.failedCount());
        RecordResult rr = result.rejectedRecords().get(0);
        assertEquals("u_name", rr.failures().get(0).ruleId());
        assertEquals("name", rr.failures().get(0).field());
        assertTrue(rr.failures().get(0).reason().contains("不唯一"));
        // 后出现的重复记录被隔离（确定性：第二次出现者失败）
        assertEquals("2", rr.recordKey());
    }

    @Test
    void rejectWhenNoRuleSetPublished() {
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(props(),
                new RuleSetManager(), new InMemoryBaselineStore(),
                new InMemoryQuarantineService());
        BatchRejectedException ex = assertThrows(BatchRejectedException.class,
                () -> pipeline.execute("b", Stream.of(Map.of("a", "1"))));
        assertEquals("RULESET_MISSING", ex.reasonCode());
    }

    @Test
    void overflowRejectStrategyDoesNotUpdateBaseline() {
        PipelineProperties p = props();
        p.setMaxRecordsPerBatch(10);
        p.setOverflowStrategy("REJECT");
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        BatchRejectedException ex = assertThrows(BatchRejectedException.class,
                () -> pipeline.execute("big",
                        IntStream.range(0, 50).mapToObj(this::rec)));
        assertEquals("CAPACITY_EXCEEDED", ex.reasonCode());
        assertTrue(store.load("age").isEmpty(), "被拒绝批次不得写入基线");
    }

    @Test
    void overflowIsolateStrategyMarksIsolatedAndSkipsBaseline() {
        PipelineProperties p = props();
        p.setMaxRecordsPerBatch(10);
        p.setOverflowStrategy("ISOLATE");
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        BatchResult result = pipeline.execute("big",
                IntStream.range(0, 50).mapToObj(this::rec));
        assertEquals(BatchStatus.ISOLATED, result.status());
        assertEquals("BATCH_ISOLATED", result.rejectReason());
        assertEquals(10, result.totalCount());
        assertTrue(store.load("age").isEmpty(), "隔离批次不得更新基线");
    }

    @Test
    void committedBatchUpdatesBaselineOnce() {
        PipelineProperties p = props();
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        pipeline.execute("b1", IntStream.range(0, 10).mapToObj(this::rec));
        Baseline afterFirst = store.load("age").orElseThrow();
        assertEquals(1, afterFirst.batchCount());
        assertEquals(10, afterFirst.totalCount());

        pipeline.execute("b2", IntStream.range(0, 10).mapToObj(this::rec));
        Baseline afterSecond = store.load("age").orElseThrow();
        assertEquals(2, afterSecond.batchCount());
        assertEquals(20, afterSecond.totalCount());
    }

    @Test
    void failedBatchDueToRecordErrorDoesNotCommit() {
        PipelineProperties p = props();
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        Stream<Map<String, String>> exploding = Stream.concat(
                IntStream.range(0, 5).mapToObj(this::rec),
                Stream.generate(() -> {
                    throw new IllegalStateException("source exploded");
                })).limit(10);

        BatchResult result = pipeline.execute("fail", exploding);
        assertEquals(BatchStatus.FAILED, result.status());
        assertEquals("BATCH_FAILED", result.rejectReason());
        assertTrue(store.load("age").isEmpty(), "执行失败的批次不得污染基线");
    }

    @Test
    void baselineCommitFailureLeavesPreviousBaselineIntact() {
        PipelineProperties p = props();
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore() {
            private int calls;
            @Override
            public void commit(Baseline next) {
                calls++;
                if (calls == 2) {
                    throw new PipelineException("BASELINE_IO_ERROR", "模拟磁盘故障");
                }
                super.commit(next);
            }
        };
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        pipeline.execute("ok", IntStream.range(0, 5).mapToObj(this::rec));
        Baseline good = store.load("age").orElseThrow();

        BatchResult failed = pipeline.execute("bad",
                IntStream.range(100, 105).mapToObj(this::rec));
        assertEquals(BatchStatus.FAILED, failed.status());
        assertEquals("BASELINE_COMMIT_FAILED", failed.rejectReason());
        Baseline unchanged = store.load("age").orElseThrow();
        assertEquals(good, unchanged, "基线提交失败必须保持已有基线不变");
    }

    @Test
    void ruleUpdateDuringLongRunningBatchDoesNotAffectInFlightBatch() throws Exception {
        PipelineProperties p = props();
        p.setRecordQueueCapacity(2);
        p.setOfferTimeoutMillis(60_000);
        RuleSetManager manager = managerWith(
                def("old", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        CountDownLatch firstEnqueued = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Stream<Map<String, String>> slow = Stream.concat(
                Stream.generate(() -> {
                    firstEnqueued.countDown();
                    await(release);
                    return Map.of("id", "x", "name", "x");
                }).limit(6),
                Stream.of(Map.of("id", "last", "name", "last")));

        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<BatchResult> future = pool.submit(
                    () -> pipeline.execute("inflight", slow));
            firstEnqueued.await();

            // 批次进行中整体替换规则集：新版本要求 phone 非空
            manager.publish(List.of(
                    def("new1", RuleType.NOT_NULL, "name", Map.of(), false),
                    def("new2", RuleType.NOT_NULL, "phone", Map.of(), false)));

            release.countDown();
            BatchResult result = future.get(30, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals("v1", result.ruleSetVersion(), "进行中批次必须锁定旧快照");
            assertEquals(BatchStatus.COMMITTED, result.status());
            assertEquals(7, result.totalCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void concurrentBatchesSameRulesProduceSameResults() throws Exception {
        PipelineProperties p = props();
        p.setMaxConcurrentBatches(8);
        RuleSetManager manager = managerWith(
                def("u", RuleType.UNIQUE, "name", Map.of(), false),
                def("r", RuleType.RANGE, "age", Map.of("min", "20", "max", "90"), true));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<? extends Future<BatchResult>> futures = IntStream.range(0, threads)
                .mapToObj(i -> pool.submit(() -> {
                    start.await();
                    return pipeline.execute("cb-" + i,
                            IntStream.range(0, 50).mapToObj(n -> {
                                Map<String, String> m = new HashMap<>();
                                m.put("id", "r" + n);
                                m.put("name", "name" + (n % 25)); // 25 个重复
                                m.put("age", String.valueOf(20 + n)); // 20..69
                                return m;
                            }));
                }))
                .toList();
        start.countDown();
        for (Future<BatchResult> f : futures) {
            BatchResult r = f.get();
            assertEquals(BatchStatus.COMMITTED, r.status());
            assertEquals(50, r.totalCount());
            assertEquals(25, r.passedCount(), "每批次独立 UNIQUE 跟踪，结论一致");
            assertEquals(25, r.failedCount());
        }
        pool.shutdown();
    }

    @Test
    void reproducibleAcrossRunsRegardlessOfOrder() {
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false),
                def("r", RuleType.RANGE, "age", Map.of("min", "0", "max", "200"), true));
        DefaultQualityPipeline p1 = new DefaultQualityPipeline(props(), manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());
        RuleSetManager manager2 = new RuleSetManager();
        manager2.publish(manager.current().definitions());
        DefaultQualityPipeline pipeline2 = new DefaultQualityPipeline(props(), manager2,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        List<Map<String, String>> records = IntStream.range(0, 40).mapToObj(n -> {
            Map<String, String> m = new HashMap<>();
            m.put("id", "r" + n);
            m.put("name", n % 7 == 0 ? "" : "n" + n);
            m.put("age", String.valueOf(n * 10));
            return m;
        }).toList();

        BatchResult first = p1.execute("run1", records.stream());
        List<Map<String, String>> shuffled = new java.util.ArrayList<>(records);
        java.util.Collections.shuffle(shuffled, new java.util.Random(42));
        BatchResult second = pipeline2.execute("run2", shuffled.stream());

        assertEquals(first.passedCount(), second.passedCount());
        assertEquals(first.failedCount(), second.failedCount());
        assertEquals(first.status(), second.status());
        List<String> keys1 = first.rejectedRecords().stream().map(RecordResult::recordKey).sorted().toList();
        List<String> keys2 = second.rejectedRecords().stream().map(RecordResult::recordKey).sorted().toList();
        assertEquals(keys1, keys2, "失败记录集合与输入顺序无关");
    }

    @Test
    void rejectRatioThresholdRejectsBatchAndKeepsBaseline() {
        PipelineProperties p = props();
        p.setMaxRejectRatio(0.2);
        p.setBaselineFields("age");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        RuleSetManager manager = managerWith(
                def("nn", RuleType.NOT_NULL, "name", Map.of(), false));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                store, new InMemoryQuarantineService());

        BatchRejectedException ex = assertThrows(BatchRejectedException.class,
                () -> pipeline.execute("r", IntStream.range(0, 10).mapToObj(n -> {
                    Map<String, String> m = new HashMap<>();
                    m.put("id", "r" + n);
                    m.put("name", n < 5 ? "" : "ok");
                    return m;
                })));
        assertEquals("REJECT_RATIO_EXCEEDED", ex.reasonCode());
        assertTrue(store.load("age").isEmpty());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
