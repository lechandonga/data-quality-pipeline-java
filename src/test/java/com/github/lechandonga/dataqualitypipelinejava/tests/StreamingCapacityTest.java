package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.BatchRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.engine.CapacityExceededException;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import com.github.lechandonga.dataqualitypipelinejava.rule.UniqueTracker;
import com.github.lechandonga.dataqualitypipelinejava.stream.BoundedRecordQueue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 大输入流式处理与容量/并发边界。
 */
class StreamingCapacityTest {

    private PipelineProperties props(int maxRecords, int queueCapacity,
                                     int uniqueCapacity, String overflow) {
        PipelineProperties p = new PipelineProperties();
        p.setMaxRecordsPerBatch(maxRecords);
        p.setRecordQueueCapacity(queueCapacity);
        p.setUniqueTrackerCapacity(uniqueCapacity);
        p.setOverflowStrategy(overflow);
        p.setOfferTimeoutMillis(2_000);
        return p;
    }

    private RuleSetManager simpleRules() {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(java.util.List.of(
                new RuleDefinition("nn", RuleType.NOT_NULL, "name",
                        java.util.Map.of(), false)));
        return manager;
    }

    @Test
    void largeStreamProcessedWithoutLoadingAllRecords() {
        // 队列极小、记录数极大：验证背压下全量处理且内存占用有界
        PipelineProperties p = props(1_000_000, 4, 1_000_000, "REJECT");
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, simpleRules(),
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        long n = 200_000;
        BatchResult result = pipeline.execute("huge",
                IntStream.range(0, (int) n).<Map<String, String>>mapToObj(i ->
                        Map.of("id", String.valueOf(i), "name", "v" + i)));

        assertEquals(BatchStatus.COMMITTED, result.status());
        assertEquals(n, result.totalCount());
        assertEquals(n, result.passedCount());
    }

    @Test
    void uniqueTrackerCapacityRejectsBatch() {
        PipelineProperties p = props(1000, 4, 5, "REJECT");
        RuleSetManager manager = new RuleSetManager();
        manager.publish(java.util.List.of(
                new RuleDefinition("u", RuleType.UNIQUE, "name",
                        java.util.Map.of(), false)));
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        BatchResult result = pipeline.execute("u",
                IntStream.range(0, 20).<Map<String, String>>mapToObj(i ->
                        Map.of("id", String.valueOf(i), "name", "v" + i)));
        // 超过唯一性跟踪槽位：批次失败，不无界占用内存
        assertEquals(BatchStatus.FAILED, result.status());
        assertEquals("BATCH_FAILED", result.rejectReason());
    }

    @Test
    void uniqueTrackerUnitBehavior() {
        UniqueTracker tracker = new UniqueTracker(2);
        RuleDefinition rule = new RuleDefinition("u", RuleType.UNIQUE, "f",
                java.util.Map.of(), false);
        assertTrue(tracker.observe(rule, "a"));
        assertFalse(tracker.observe(rule, "a"));
        assertTrue(tracker.observe(rule, "b"));
        assertThrows(CapacityExceededException.class,
                () -> tracker.observe(rule, "c"));
    }

    @Test
    void queueCloseOnFullQueueWaitsThenDeliversPoison() throws Exception {
        BoundedRecordQueue queue = new BoundedRecordQueue(2, 200);
        queue.put(Map.of("a", "1"));
        queue.put(Map.of("a", "2"));

        // 满队列下 close 会等待空位；用消费者线程排空，验证哨兵可靠送达
        var closer = java.util.concurrent.Executors.newSingleThreadExecutor();
        try {
            var closed = closer.submit(() -> {
                queue.close();
                return true;
            });
            assertEquals(Map.of("a", "1"), queue.poll());
            assertEquals(Map.of("a", "2"), queue.poll());
            assertTrue(closed.get(5, java.util.concurrent.TimeUnit.SECONDS));
            assertNull(queue.poll(), "满队列关闭后消费者排空数据必须收到哨兵");
        } finally {
            closer.shutdownNow();
        }
    }

    @Test
    void queueAbortWakesBlockedProducer() throws Exception {
        BoundedRecordQueue queue = new BoundedRecordQueue(1, 60_000);
        queue.put(Map.of("a", "1"));

        var pool = java.util.concurrent.Executors.newFixedThreadPool(1);
        try {
            var blockedPut = pool.submit(() -> {
                queue.put(Map.of("a", "2"));
                return true;
            });
            Thread.sleep(200);
            assertFalse(blockedPut.isDone(), "队列满时生产者必须被背压阻塞");
            queue.abort();
            // abort 必须唤醒阻塞生产者，使其快速失败而不是等待 60s 超时
            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> blockedPut.get(5, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void queueCloseIsIdempotent() throws Exception {
        BoundedRecordQueue queue = new BoundedRecordQueue(4, 200);
        queue.close();
        queue.close();
        assertNull(queue.poll());
    }

    @Test
    void concurrentBatchLimitIsEnforced() throws Exception {
        PipelineProperties p = props(1000, 2, 1000, "REJECT");
        p.setMaxConcurrentBatches(1);
        p.setOfferTimeoutMillis(30_000);
        RuleSetManager manager = simpleRules();
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, manager,
                new InMemoryBaselineStore(), new InMemoryQuarantineService());

        var release = new java.util.concurrent.CountDownLatch(1);
        var started = new java.util.concurrent.CountDownLatch(1);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            // 第一个批次占住唯一许可
            var first = pool.submit(() -> pipeline.execute("first",
                    Stream.concat(
                            Stream.of(Map.of("id", "0", "name", "0")),
                            IntStream.range(1, 50).<Map<String, String>>mapToObj(i -> {
                                started.countDown();
                                try { release.await(); } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                }
                                return Map.of("id", String.valueOf(i), "name", "v");
                            }))));
            started.await();

            AtomicLong secondBeganAt = new AtomicLong(-1);
            var second = pool.submit(() -> {
                secondBeganAt.set(System.nanoTime());
                return pipeline.execute("second",
                        IntStream.range(0, 10).<Map<String, String>>mapToObj(i ->
                                Map.of("id", "s" + i, "name", "v")));
            });
            // 第二批在 acquireSlot 排队等待，300ms 内不可能结束
            Thread.sleep(300);
            assertFalse(second.isDone(), "并发上限为 1 时第二批必须排队，不能并发执行");
            release.countDown();
            BatchResult r1 = first.get(30, java.util.concurrent.TimeUnit.SECONDS);
            BatchResult r2 = second.get(30, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(BatchStatus.COMMITTED, r1.status());
            assertEquals(BatchStatus.COMMITTED, r2.status());
            assertEquals(10, r2.totalCount());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void rejectBatchLeavesNoPartialBaseline() {
        PipelineProperties p = props(5, 2, 1000, "REJECT");
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(p, simpleRules(),
                store, new InMemoryQuarantineService());
        assertThrows(BatchRejectedException.class, () -> pipeline.execute("x",
                IntStream.range(0, 50).<Map<String, String>>mapToObj(i ->
                        Map.of("id", String.valueOf(i), "name", "v"))));
        assertEquals(0, store.size());
    }
}
