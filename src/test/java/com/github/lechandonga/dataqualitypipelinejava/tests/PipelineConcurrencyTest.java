package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleViolation;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineCapacityException;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineConcurrencyTest {

    private RuleRegistry registry;
    private InMemoryBaselineStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryBaselineStore();
        registry = new RuleRegistry(new RuleCompiler());
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "1000000"),
                unique("u-id", List.of("id"))));
    }

    private PipelineService pipeline(PipelineProperties props) {
        return new PipelineService(registry, new BaselineService(store), props);
    }

    private PipelineProperties props(int workers, int queue, int distinct, OverflowPolicy policy) {
        return new PipelineProperties(workers, 2, queue, distinct, policy, 3.0, 0.3, null);
    }

    private List<DataRecord> dataset(int n) {
        List<DataRecord> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Map<String, String> d = new LinkedHashMap<>();
            d.put("id", "id-" + i);
            d.put("v", String.valueOf(i));
            out.add(rec(i, "rec-" + i, d));
        }
        return out;
    }

    /** 深拷贝数据集，保证重复执行/并发批次间输入互不污染。 */
    private List<DataRecord> copy(List<DataRecord> src) {
        List<DataRecord> out = new ArrayList<>();
        for (DataRecord r : src) {
            out.add(rec(r.sourceOrder(), r.id(), new LinkedHashMap<>(r.data())));
        }
        return out;
    }

    private List<String> summary(BatchResult r) {
        List<String> out = new ArrayList<>();
        for (RuleViolation v : r.violations()) {
            out.add(v.recordId() + ":" + v.status()
                    + v.reasons().stream().map(x -> x.ruleId()).sorted().toList());
        }
        return out;
    }

    @Test
    @Timeout(60)
    void resultsAreIdenticalAcrossWorkerCountsAndRepeatedRuns() {
        int n = 5_000;
        // 显式重置规则集（本类其他用例会 publish 自定义规则）
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "1000000"),
                unique("u-id", List.of("id"))));
        List<String> expected = null;
        for (int workers : new int[]{1, 2, 4, 8, 16}) {
            PipelineService p = pipeline(props(workers, 512, 2_000_000, OverflowPolicy.ISOLATE));
            for (int run = 0; run < 2; run++) {
                // 每次执行使用全新的等价数据集
                List<DataRecord> data = dataset(n);
                data.set(10, data.get(10).withField("v", "9999999")); // 超范围
                data.set(20, data.get(20).withField("id", "id-0"));   // 与 rec-0 重复
                data.set(30, data.get(30).withField("v", null));      // 缺失
                BatchResult r = p.execute(new ExecutionRequest("b-" + workers + "-" + run,
                        "ds", false, workers, null), data.iterator());
                List<String> got = summary(r);
                if (expected == null) {
                    expected = got;
                    assertEquals(n, r.total());
                    assertEquals(3, n - r.passedCount());
                } else {
                    assertEquals(expected, got,
                            "workers=" + workers + " run=" + run + " must produce identical verdicts");
                }
            }
        }
    }

    @Test
    @Timeout(60)
    void backpressureKeepsAllRecordsAndNoSpuriousIsolation() {
        // worker 被屏障确定性占住 -> 队列被打满 -> 摄入背压等待；放开后全部正常处理
        int workers = 2;
        int queue = 4;
        PipelineService p = pipeline(props(workers, queue, 2_000_000, OverflowPolicy.ISOLATE));
        CyclicBarrier barrier = new CyclicBarrier(workers);
        registry.publish(new com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet(
                "barrier", List.of(new BarrierRule("barrier", barrier, workers)), List.of()));

        List<DataRecord> data = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            data.add(rec(i, "rec-" + i, Map.of("v", "1")));
        }
        BatchResult r = p.execute(ExecutionRequest.of("b1", "ds", false), data.iterator());
        assertEquals(BatchStatus.ACCEPTED, r.status());
        assertEquals(100, r.total());
        assertEquals(100, r.passedCount(), "backpressure must not isolate healthy records");
        assertEquals(0, r.overflowIsolatedCount());
    }

    @Test
    @Timeout(60)
    void uniqueStateCapacityIsolatesWhenBounded() {
        PipelineService p = pipeline(props(4, 100, 50, OverflowPolicy.ISOLATE));
        BatchResult r = p.execute(ExecutionRequest.of("b1", "ds", false), dataset(200).iterator());
        assertTrue(r.overflowIsolatedCount() > 0, "records beyond unique-state capacity are isolated");
        // 前 50 个唯一键以内的记录结论正常
        assertEquals(50, r.violations().subList(0, 50).stream()
                .filter(v -> v.status().name().equals("PASSED")).count());
    }

    @Test
    @Timeout(60)
    void uniqueStateCapacityRejectsWholeBatchUnderRejectPolicy() {
        PipelineService p = pipeline(props(4, 1_000, 10, OverflowPolicy.REJECT));
        BatchResult r = p.execute(ExecutionRequest.of("b1", "ds", false), dataset(50).iterator());
        assertEquals(BatchStatus.REJECTED, r.status());
        assertTrue(r.rejectReason().contains("unique-state capacity"));
    }

    @Test
    @Timeout(90)
    void sameRulesConcurrentBatchesProduceConsistentIndependentResults() throws Exception {
        PipelineProperties p2 = new PipelineProperties(4, 16, 1_000, 2_000_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null);
        PipelineService p = new PipelineService(registry, new BaselineService(store), p2);
        List<DataRecord> data = dataset(2_000);

        int threads = 12;
        ExecutorService exec = Executors.newFixedThreadPool(threads);
        List<Future<BatchResult>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int idx = t;
            futures.add(exec.submit(() -> p.execute(
                    new ExecutionRequest("batch-" + idx, "shared-ds", false, null, null),
                    copy(data).iterator())));
        }
        List<String> expected = summary(futures.get(0).get(60, TimeUnit.SECONDS));
        for (Future<BatchResult> f : futures) {
            BatchResult r = f.get(60, TimeUnit.SECONDS);
            assertEquals(2_000, r.total());
            assertEquals(expected, summary(r), "concurrent batches must see identical conclusions");
        }
        exec.shutdown();
    }

    @Test
    @Timeout(30)
    void concurrentBatchLimitIsEnforced() throws Exception {
        PipelineProperties p2 = new PipelineProperties(1, 2, 1_000, 2_000_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null);
        PipelineService p = new PipelineService(registry, new BaselineService(store), p2);
        registry.publish(new com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet(
                "slow", List.of(new SlowRule("slow", 300)), List.of()));

        int accepts = 0;
        int rejects = 0;
        ExecutorService exec = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        // 8 个批次同时发起，上限 2：至少有一些必须被拒绝，且任何时刻不超过 2 个执行中
        for (int i = 0; i < 8; i++) {
            final int idx = i;
            futures.add(exec.submit(() -> {
                start.await();
                try {
                    p.execute(ExecutionRequest.of("c" + idx, "ds", false),
                            List.of(rec(0, "x", Map.of("v", "1"))).iterator());
                } catch (PipelineCapacityException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            try {
                f.get(30, TimeUnit.SECONDS);
                accepts++;
            } catch (Exception e) {
                if (e.getCause() instanceof RuntimeException re
                        && re.getCause() instanceof PipelineCapacityException) {
                    rejects++;
                } else {
                    throw e;
                }
            }
        }
        exec.shutdown();
        assertTrue(rejects > 0, "at least some batches must be rejected, rejects=" + rejects);
        assertEquals(8, accepts + rejects);
    }
}
