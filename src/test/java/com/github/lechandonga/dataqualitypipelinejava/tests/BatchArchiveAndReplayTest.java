package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchiveCorruptedException;
import com.github.lechandonga.dataqualitypipelinejava.archive.FileBatchArchiveStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.PersistenceStatus;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.BatchReplayRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayReport;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 批次存档 + 隔离导出/修复重放闭环的行为测试。
 */
class BatchArchiveAndReplayTest {

    private static PipelineProperties props(String archiveDir, int maxInMemory) {
        return new PipelineProperties(4, 8, 256, 1_000_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null, archiveDir, maxInMemory);
    }

    private static List<DataRecord> sampleInput() {
        return List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "999"),
                recV(2, "rec-2", "id-2", null),
                recV(3, "rec-3", "id-0", "10"));
    }

    private static DataRecord recV(long order, String recId, String id, String v) {
        Map<String, String> d = new LinkedHashMap<>();
        d.put("key", id);
        if (v != null) {
            d.put("v", v);
        }
        return rec(order, recId, d);
    }

    private static RuleRegistry registryWithRules() {
        RuleRegistry registry = new RuleRegistry(new RuleCompiler());
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "100"),
                unique("u-key", List.of("key"))));
        return registry;
    }

    private static PipelineService service(RuleRegistry registry, String archiveDir,
                                           int maxInMemory) {
        return new PipelineService(registry,
                new BaselineService(new InMemoryBaselineStore()),
                props(archiveDir, maxInMemory),
                archiveDir == null ? null : new FileBatchArchiveStore(archiveDir));
    }

    private static BatchResult runFresh(RuleRegistry registry, String batchId,
                                        List<DataRecord> records) {
        return service(registry, null, 100)
                .execute(ExecutionRequest.of(batchId, "ds", false), records.iterator());
    }

    private static List<String> signature(BatchResult r) {
        return r.violations().stream()
                .map(v -> v.recordId() + "|" + v.status() + "|"
                        + v.reasons().stream().map(f -> f.ruleId() + ":" + f.field())
                        .sorted().toList())
                .toList();
    }

    private static void assertSameConclusions(BatchResult expected, BatchResult actual) {
        assertEquals(expected.status(), actual.status());
        assertEquals(expected.total(), actual.total());
        assertEquals(expected.passedCount(), actual.passedCount());
        assertEquals(expected.quarantinedCount(), actual.quarantinedCount());
        assertEquals(expected.overflowIsolatedCount(), actual.overflowIsolatedCount());
        assertEquals(signature(expected), signature(actual));
    }

    private static Path archiveFile(Path dir, String batchId) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> p.getFileName().toString().startsWith(batchId + "-"))
                    .findFirst().orElseThrow();
        }
    }

    // ---------- 跨重启查询与重放 ----------

    @Test
    void restartedInstanceReadsCompleteConclusionFromArchive(@TempDir Path dir) {
        RuleRegistry registry = registryWithRules();
        BatchResult original = service(registry, dir.toString(), 100).execute(
                ExecutionRequest.of("b-restart", "ds", false), sampleInput().iterator());
        assertEquals(PersistenceStatus.PERSISTED, original.persistenceStatus());

        PipelineService restarted = service(registryWithRules(), dir.toString(), 100);
        assertInstanceOf(PipelineService.Lookup.NotFound.class,
                restarted.lookup("never-existed"));
        BatchResult loaded = ((PipelineService.Lookup.Found)
                restarted.lookup("b-restart")).result();
        assertSameConclusions(original, loaded);
        assertEquals(PersistenceStatus.PERSISTED, loaded.persistenceStatus());
        assertEquals(original.rulesVersion(), loaded.rulesVersion());
    }

    @Test
    void quarantinedBatchCanBeReplayedAfterRestart(@TempDir Path dir) {
        service(registryWithRules(), dir.toString(), 100).execute(
                ExecutionRequest.of("b-replay", "ds", false), sampleInput().iterator());

        PipelineService restarted = service(registryWithRules(), dir.toString(), 100);
        ReplayReport report = restarted.replayWithReport("b-replay", List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20")).iterator());
        assertFalse(report.rejected());
        assertEquals(List.of("rec-1", "rec-2"), report.repairedPassed());
        assertEquals(List.of("rec-3"), report.stillQuarantined());
        assertEquals(3L, report.result().passedCount());

        // 重放后的结论也已落盘：再次重启仍完整可查
        PipelineService third = service(registryWithRules(), dir.toString(), 100);
        assertSameConclusions(report.result(),
                ((PipelineService.Lookup.Found) third.lookup("b-replay")).result());
    }

    @Test
    void replayAfterRestartIsRefusedWhenRulesVersionChanged(@TempDir Path dir) {
        service(registryWithRules(), dir.toString(), 100).execute(
                ExecutionRequest.of("b-version", "ds", false), sampleInput().iterator());

        RuleRegistry changed = new RuleRegistry(new RuleCompiler());
        changed.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "10"),
                unique("u-key", List.of("key"))));
        PipelineService restarted = service(changed, dir.toString(), 100);
        assertThrows(IllegalStateException.class,
                () -> restarted.replayWithReport("b-version",
                        List.of(recV(1, "rec-1", "id-1", "5")).iterator()));
    }

    // ---------- 内存逐出 ----------

    @Test
    void oldestBatchesEvictedPastLimitButRemainReachableFromArchive(@TempDir Path dir) {
        RuleRegistry registry = registryWithRules();
        PipelineService pipeline = service(registry, dir.toString(), 2);
        for (int i = 0; i < 5; i++) {
            pipeline.execute(ExecutionRequest.of("b-" + i, "ds", false),
                    List.of(recV(0, "r0", "k" + i, "10")).iterator());
        }
        assertSameConclusions(
                runFresh(registry, "cmp", List.of(recV(0, "r0", "k0", "10"))),
                ((PipelineService.Lookup.Found) pipeline.lookup("b-0")).result());
        assertInstanceOf(PipelineService.Lookup.Found.class, pipeline.lookup("b-4"));

        // 逐出后（limit=1 立即逐出）仍可经存档重放
        PipelineService tight = service(registry, dir.toString(), 1);
        tight.execute(ExecutionRequest.of("b-q", "ds", false), sampleInput().iterator());
        ReplayReport report = tight.replayWithReport("b-q",
                List.of(recV(1, "rec-1", "id-1", "50")).iterator());
        assertEquals(List.of("rec-1"), report.repairedPassed());
    }

    @Test
    void evictedBatchWithoutArchiveIsDistinguishableFromNeverExisted() {
        PipelineService pipeline = service(registryWithRules(), null, 2);
        for (int i = 0; i < 4; i++) {
            pipeline.execute(ExecutionRequest.of("m-" + i, "ds", false),
                    List.of(recV(0, "r", "k" + i, "10")).iterator());
        }
        assertInstanceOf(PipelineService.Lookup.Evicted.class, pipeline.lookup("m-0"));
        assertInstanceOf(PipelineService.Lookup.NotFound.class, pipeline.lookup("m-x"));
        assertThrows(IllegalStateException.class,
                () -> pipeline.exportQuarantined("m-0"));
        assertInstanceOf(PipelineService.Lookup.Found.class, pipeline.lookup("m-3"));
    }

    @Test
    void evictionUnderConcurrencyNeverLosesInUseBatches(@TempDir Path dir) throws Exception {
        PipelineService pipeline = service(registryWithRules(), dir.toString(), 1);
        int threads = 6;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int idx = t;
            futures.add(pool.submit(() -> {
                try {
                    start.await();
                    String batchId = "c-" + idx;
                    BatchResult r = pipeline.execute(
                            ExecutionRequest.of(batchId, "ds", false), sampleInput().iterator());
                    BatchResult seen = ((PipelineService.Lookup.Found)
                            pipeline.lookup(batchId)).result();
                    assertEquals(4L, seen.total());
                    assertEquals(signature(r), signature(seen));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get();
        }
        pool.shutdown();
    }

    // ---------- 落盘失败与存档损坏 ----------

    @Test
    void archiveWriteFailureKeepsConclusionButMarksItNotPersisted(@TempDir Path dir)
            throws Exception {
        FileBatchArchiveStore failingStore = new FileBatchArchiveStore(dir.toString());
        failingStore.armNextSaveFailure();
        PipelineService pipeline = new PipelineService(registryWithRules(),
                new BaselineService(new InMemoryBaselineStore()),
                props(dir.toString(), 100), failingStore);

        BatchResult result = pipeline.execute(
                ExecutionRequest.of("b-fail", "ds", false), sampleInput().iterator());
        assertEquals(4L, result.total());
        assertEquals(1L, result.passedCount());
        assertEquals(PersistenceStatus.NOT_PERSISTED, result.persistenceStatus());
        // 磁盘上不得留下残档
        try (Stream<Path> files = Files.list(dir)) {
            assertEquals(0L, files.filter(p ->
                    p.getFileName().toString().startsWith("b-fail-")).count());
        }
        // 内存中仍可查；重启后视为不存在（明确的 NOT_PERSISTED 语义）
        assertInstanceOf(PipelineService.Lookup.Found.class, pipeline.lookup("b-fail"));
        PipelineService restarted = service(registryWithRules(), dir.toString(), 100);
        assertInstanceOf(PipelineService.Lookup.NotFound.class,
                restarted.lookup("b-fail"));
    }

    @Test
    void corruptedArchiveIsReportedExplicitlyNotSilentlyMissing(@TempDir Path dir)
            throws Exception {
        service(registryWithRules(), dir.toString(), 100).execute(
                ExecutionRequest.of("b-bad", "ds", false), sampleInput().iterator());
        Path file = archiveFile(dir, "b-bad");
        Files.writeString(file, "{ this is not valid json ");

        PipelineService restarted = service(registryWithRules(), dir.toString(), 100);
        assertInstanceOf(PipelineService.Lookup.Corrupt.class, restarted.lookup("b-bad"));
        assertThrows(BatchArchiveCorruptedException.class,
                () -> restarted.exportQuarantined("b-bad"),
                "corrupted archive must fail loudly rather than look like absence");
        // 空文件同样算损坏
        Files.writeString(file, "");
        assertInstanceOf(PipelineService.Lookup.Corrupt.class, restarted.lookup("b-bad"));
    }

    // ---------- 导出 / 原样重放 / 修复等价性 ----------

    @Test
    void exportingQuarantineAndReplayingUnchangedYieldsIdenticalConclusion(@TempDir Path dir) {
        PipelineService pipeline = service(registryWithRules(), dir.toString(), 100);
        BatchResult before = pipeline.execute(
                ExecutionRequest.of("b-export", "ds", false), sampleInput().iterator());

        // 导出：记录标识、原始顺序、原始字段一字不改
        List<DataRecord> exported = pipeline.exportQuarantined("b-export");
        assertEquals(List.of("rec-1", "rec-2", "rec-3"),
                exported.stream().map(DataRecord::id).toList());
        assertEquals(1L, exported.get(0).sourceOrder());
        assertEquals("999", exported.get(0).field("v"));
        assertFalse(exported.get(1).data().containsKey("v"));

        // 原样直接回灌重放：整批结论与导出前逐条一致
        ReplayReport report = pipeline.replayWithReport(
                "b-export", exported.iterator());
        assertFalse(report.rejected());
        assertEquals(List.of(), report.repairedPassed());
        assertEquals(List.of("rec-1", "rec-2", "rec-3"), report.stillQuarantined());
        assertSameConclusions(before, report.result());
    }

    @Test
    void repairedReplayIsEquivalentToRunningTheWholeCurrentBatchFresh(@TempDir Path dir) {
        RuleRegistry registry = registryWithRules();
        PipelineService pipeline = service(registry, dir.toString(), 100);
        pipeline.execute(ExecutionRequest.of("b-equiv", "ds", false),
                sampleInput().iterator());

        // 当前这批记录：rec-0 仍通过，rec-1/rec-2 已修复，rec-3 仍未修复
        List<DataRecord> currentBatch = List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20"),
                recV(3, "rec-3", "id-0", "10"));

        ReplayReport report = pipeline.replayWithReport("b-equiv", List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20")).iterator());
        BatchResult freshFullRun = runFresh(registry, "fresh", currentBatch);
        assertSameConclusions(freshFullRun, report.result());

        // 再修复 rec-3（改掉与 rec-0 冲突的键）后，仍等价于整批从头跑
        List<DataRecord> fullyFixed = List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20"),
                recV(3, "rec-3", "id-3", "10"));
        ReplayReport finalReport = pipeline.replayWithReport("b-equiv",
                List.of(recV(3, "rec-3", "id-3", "10")).iterator());
        assertSameConclusions(runFresh(registry, "fresh2", fullyFixed),
                finalReport.result());
        assertEquals(4L, finalReport.result().passedCount());
    }

    @Test
    void recordsOutsideQuarantineListRejectWholeInputAtomically(@TempDir Path dir) {
        PipelineService pipeline = service(registryWithRules(), dir.toString(), 100);
        BatchResult before = pipeline.execute(
                ExecutionRequest.of("b-guard", "ds", false), sampleInput().iterator());

        // 混入：一个真正在隔离清单的修复 + 一个已通过记录 + 一个从未存在的记录
        List<DataRecord> mixed = List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(0, "rec-0", "id-0", "10"),
                recV(9, "rec-9", "id-9", "70"));
        BatchReplayRejectedException ex = assertThrows(BatchReplayRejectedException.class,
                () -> pipeline.replayWithReport("b-guard", mixed.iterator()));
        assertEquals("NOT_IN_QUARANTINE_LIST", ex.reasonCode());
        assertEquals(List.of("rec-0", "rec-9"), ex.rejectedRecordIds());

        // 整体不生效：批次结论与计数一字未变，已通过记录未被改动
        BatchResult after = ((PipelineService.Lookup.Found)
                pipeline.lookup("b-guard")).result();
        assertSameConclusions(before, after);
        assertEquals(PersistenceStatus.PERSISTED, after.persistenceStatus());

        // 之后合法的重放仍然可用（拒绝没有污染批次状态）
        ReplayReport ok = pipeline.replayWithReport("b-guard",
                List.of(recV(1, "rec-1", "id-1", "50")).iterator());
        assertEquals(2L, ok.result().passedCount());
    }
}
