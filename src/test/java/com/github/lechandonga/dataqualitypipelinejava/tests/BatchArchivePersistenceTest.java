package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.archive.ArchiveException;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchArchive;
import com.github.lechandonga.dataqualitypipelinejava.archive.BatchSnapshot;
import com.github.lechandonga.dataqualitypipelinejava.archive.FileBatchArchive;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineService;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.DataRecord;
import com.github.lechandonga.dataqualitypipelinejava.model.OverflowPolicy;
import com.github.lechandonga.dataqualitypipelinejava.model.PersistenceStatus;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.BatchLookup;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ExecutionRequest;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.PipelineService;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayOutcome;
import com.github.lechandonga.dataqualitypipelinejava.pipeline.ReplayRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.rec;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批次存档、内存逐出、隔离导出与修复重放闭环的端到端测试。
 */
class BatchArchivePersistenceTest {

    @TempDir
    Path tempDir;

    private RuleRegistry registry;

    private PipelineProperties props(String archiveDir, int maxRetained) {
        return new PipelineProperties(4, 8, 256, 1_000_000,
                OverflowPolicy.ISOLATE, 3.0, 0.3, null, archiveDir, maxRetained);
    }

    private PipelineService service(String archiveDir, int maxRetained) {
        BatchArchive archive = archiveDir == null
                ? new com.github.lechandonga.dataqualitypipelinejava.archive.InMemoryBatchArchive()
                : new FileBatchArchive(archiveDir);
        return new PipelineService(registry, new BaselineService(new InMemoryBaselineStore()),
                props(archiveDir, maxRetained), archive);
    }

    /** 模拟重启：新 Registry/新服务；规则重新发布为相同内容，因此版本号相同。 */
    private PipelineService restart(String archiveDir, int maxRetained) {
        registry = new RuleRegistry(new RuleCompiler());
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "100"),
                unique("u-id", List.of("id"))));
        return service(archiveDir, maxRetained);
    }

    @BeforeEach
    void setUp() {
        registry = new RuleRegistry(new RuleCompiler());
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "100"),
                unique("u-id", List.of("id"))));
    }

    private List<DataRecord> sample() {
        return List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "999"),
                recV(2, "rec-2", "id-2", null),
                recV(3, "rec-3", "id-0", "10"));
    }

    private DataRecord recV(long order, String recId, String id, String v) {
        Map<String, String> d = new LinkedHashMap<>();
        d.put("id", id);
        if (v != null) {
            d.put("v", v);
        }
        return rec(order, recId, d);
    }

    private static List<String> verdicts(BatchResult r) {
        List<String> out = new ArrayList<>();
        r.violations().forEach(v -> out.add(v.recordId() + ":" + v.status()
                + v.reasons().stream().map(x -> x.ruleId()).sorted().toList()));
        return out;
    }

    @Test
    void resultPersistedAndSurvivesRestartWithIdenticalVerdict() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService first = service(dir, 100);
        BatchResult executed = first.execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());
        assertEquals(PersistenceStatus.PERSISTED, executed.persistenceStatus());

        PipelineService second = restart(dir, 100);
        BatchLookup lookup = second.lookup("b1");
        assertInstanceOf(BatchLookup.Found.class, lookup);
        BatchResult afterRestart = ((BatchLookup.Found) lookup).result();
        assertEquals(verdicts(executed), verdicts(afterRestart));
        assertEquals(executed.status(), afterRestart.status());
        assertEquals(executed.passedCount(), afterRestart.passedCount());
        assertEquals(executed.quarantinedCount(), afterRestart.quarantinedCount());
        assertEquals(executed.rulesVersion(), afterRestart.rulesVersion());

        ReplayOutcome outcome = second.replay("b1", List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20")).iterator());
        assertEquals(Set.of("rec-1", "rec-2"), new HashSet<>(outcome.nowPassed()));
        assertEquals(List.of("rec-3"), outcome.stillQuarantined());
        assertEquals(3, outcome.batch().passedCount());
        assertEquals(1, outcome.batch().quarantinedCount());
        assertEquals(PersistenceStatus.PERSISTED, outcome.batch().persistenceStatus());
    }

    @Test
    void replayAfterRestartIsRefusedWhenRulesVersionChanged() {
        String dir = tempDir.resolve("archive").toString();
        service(dir, 100).execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());

        PipelineService second = service(dir, 100);
        registry.replaceAll(List.of(
                notNull("nn-v", "v"),
                range("r-v", "v", "0", "10"),
                unique("u-id", List.of("id"))));
        assertInstanceOf(BatchLookup.Found.class, second.lookup("b1"));
        ReplayRejectedException ex = assertThrows(ReplayRejectedException.class,
                () -> second.replay("b1", List.of(recV(1, "rec-1", "id-1", "5")).iterator()));
        assertEquals("RULES_VERSION_MISMATCH", ex.reason());
    }

    @Test
    void evictedBatchIsStillQueryableAndReplayableFromArchive() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 2);
        for (int i = 1; i <= 4; i++) {
            svc.execute(ExecutionRequest.of("b" + i, "ds", false), sample().iterator());
        }
        assertFalse(svc.resultOf("b1").isPresent(), "b1 must be evicted from memory");

        BatchLookup lookup = svc.lookup("b1");
        assertInstanceOf(BatchLookup.Found.class, lookup,
                "evicted-but-archived batch must still be queryable");
        assertEquals(4, ((BatchLookup.Found) lookup).result().total());

        ReplayOutcome outcome = svc.replay("b1", List.of(
                recV(1, "rec-1", "id-1", "50")).iterator());
        assertEquals(List.of("rec-1"), outcome.nowPassed());
        assertEquals(Set.of("rec-2", "rec-3"), new HashSet<>(outcome.stillQuarantined()));
    }

    @Test
    void evictedBatchWithoutArchiveIsDistinguishableFromNeverExisted() {
        PipelineService svc = service(null, 2);
        for (int i = 1; i <= 4; i++) {
            BatchResult r = svc.execute(
                    ExecutionRequest.of("b" + i, "ds", false), sample().iterator());
            assertEquals(PersistenceStatus.IN_MEMORY_ONLY, r.persistenceStatus());
        }
        assertInstanceOf(BatchLookup.Evicted.class, svc.lookup("b1"),
                "evicted without archive must report EVICTED, not an empty verdict");
        assertInstanceOf(BatchLookup.Unknown.class, svc.lookup("never-ran"),
                "unknown batch must stay distinguishable from evicted");

        ReplayRejectedException ex = assertThrows(ReplayRejectedException.class,
                () -> svc.replay("b1", List.of(recV(1, "rec-1", "id-1", "5")).iterator()));
        assertEquals("BATCH_EVICTED", ex.reason());
        assertThrows(ReplayRejectedException.class, () -> svc.exportQuarantined("b1"));
    }

    @Test
    void persistenceFailureMarksNotPersistedButKeepsVerdict() {
        String dir = tempDir.resolve("failing").toString();
        BatchArchive failing = new FileBatchArchive(dir) {
            @Override
            public void save(BatchSnapshot snapshot) {
                throw new ArchiveException("simulated disk failure");
            }
        };
        PipelineService svc = new PipelineService(registry,
                new BaselineService(new InMemoryBaselineStore()), props(dir, 100), failing);

        BatchResult result = svc.execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());
        assertEquals(BatchStatus.ACCEPTED, result.status());
        assertEquals(4, result.total(), "execution verdict itself must be returned intact");
        assertEquals(PersistenceStatus.NOT_PERSISTED, result.persistenceStatus(),
                "failed persistence must be explicitly distinguishable, never falsely PERSISTED");

        // 内存里结论仍可用；磁盘上不存在该批次存档
        assertInstanceOf(BatchLookup.Found.class, svc.lookup("b1"));
        assertTrue(new FileBatchArchive(dir).load("b1").isEmpty());
    }

    @Test
    void corruptedArchiveFailsLoudlyOnQueryAndReplay() throws IOException {
        String dir = tempDir.resolve("archive").toString();
        service(dir, 100).execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());

        Path file = tempDir.resolve("archive").resolve("b1.batch.json");
        Files.writeString(file, "{ truncated, not valid json ");

        PipelineService svc = service(dir, 100);
        assertInstanceOf(BatchLookup.Corrupted.class, svc.lookup("b1"),
                "corrupted archive must never be silently treated as absent");
        assertThrows(ArchiveException.class,
                () -> new FileBatchArchive(dir).load("b1"));
        assertThrows(ArchiveException.class, () -> svc.exportQuarantined("b1"));
    }

    @Test
    void exportedQuarantineReplayedAsIsYieldsIdenticalVerdict() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 100);
        BatchResult original = svc.execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());

        List<DataRecord> exported = svc.exportQuarantined("b1");
        assertEquals(List.of("rec-1", "rec-2", "rec-3"),
                exported.stream().map(DataRecord::id).toList(),
                "export preserves original order");

        ReplayOutcome same = svc.replay("b1", exported.iterator());
        assertEquals(verdicts(original), verdicts(same.batch()),
                "verbatim export replayed as-is must reproduce the verdict record by record");
        assertTrue(same.nowPassed().isEmpty());
        assertEquals(Set.of("rec-1", "rec-2", "rec-3"),
                new HashSet<>(same.stillQuarantined()));

        assertEquals("999", exported.get(0).field("v"));
        assertEquals(List.of("id", "v"), new ArrayList<>(exported.get(0).data().keySet()));
        assertFalse(exported.get(1).data().containsKey("v"), "missing field stays missing");
    }

    @Test
    void repairedReplayIsEquivalentToFullRerunWithCurrentRecords() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 100);
        svc.execute(ExecutionRequest.of("b1", "ds", false), sample().iterator());

        List<DataRecord> repaired = List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20"));
        BatchResult replayed = svc.replay("b1", repaired.iterator()).batch();

        List<DataRecord> currentBatch = List.of(
                recV(0, "rec-0", "id-0", "10"),
                recV(1, "rec-1", "id-1", "50"),
                recV(2, "rec-2", "id-2", "20"),
                recV(3, "rec-3", "id-0", "10"));
        BatchResult rerun = svc.execute(
                ExecutionRequest.of("full-rerun", "ds", false), currentBatch.iterator());

        assertEquals(verdicts(rerun), verdicts(replayed),
                "post-replay verdict must equal a full fresh run over the current records");
    }

    @Test
    void foreignAndDuplicateReplayRecordsAreRejectedAsAWhole() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 100);
        BatchResult before = svc.execute(
                ExecutionRequest.of("b1", "ds", false), sample().iterator());

        // 混入一个已通过记录（rec-0）和一个根本不存在的记录 id（stranger）
        List<DataRecord> mixed = List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(0, "rec-0", "id-0", "10"),
                recV(9, "stranger", "id-9", "1"));
        ReplayRejectedException ex = assertThrows(ReplayRejectedException.class,
                () -> svc.replay("b1", mixed.iterator()));
        assertEquals("NOT_IN_QUARANTINE", ex.reason());
        // 整体不生效：批次结论一点没变，陌生人没有成为新记录
        BatchResult unchanged = ((BatchLookup.Found) svc.lookup("b1")).result();
        assertEquals(verdicts(before), verdicts(unchanged));
        assertEquals(4, unchanged.total());

        // 同一隔离 id 在输入中重复，同样整体拒绝
        List<DataRecord> duplicated = List.of(
                recV(1, "rec-1", "id-1", "50"),
                recV(1, "rec-1", "id-1", "51"));
        ReplayRejectedException dup = assertThrows(ReplayRejectedException.class,
                () -> svc.replay("b1", duplicated.iterator()));
        assertEquals("NOT_IN_QUARANTINE", dup.reason());
        assertEquals(verdicts(before),
                verdicts(((BatchLookup.Found) svc.lookup("b1")).result()));
    }

    @Test
    void emptyReplayIsRejected() {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 100);
        svc.execute(ExecutionRequest.of("b1", "ds", false), sample().iterator());
        ReplayRejectedException ex = assertThrows(ReplayRejectedException.class,
                () -> svc.replay("b1", java.util.Collections.emptyIterator()));
        assertEquals("EMPTY", ex.reason());
    }

    @Test
    void concurrentExecutionEvictionQueryAndReplayNeverSeePartialState() throws Exception {
        String dir = tempDir.resolve("archive").toString();
        PipelineService svc = service(dir, 3);
        // 先放 2 个批次
        for (int i = 1; i <= 2; i++) {
            svc.execute(ExecutionRequest.of("b" + i, "ds", false), sample().iterator());
        }
        java.util.concurrent.ExecutorService pool =
                java.util.concurrent.Executors.newFixedThreadPool(8);
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        // 持续制造新批次触发逐出，同时并发查询/导出/重放新旧批次
        for (int t = 0; t < 6; t++) {
            final int idx = t;
            futures.add(pool.submit(() -> {
                for (int i = 0; i < 10; i++) {
                    String bid = "c-" + idx + "-" + i;
                    BatchResult r = svc.execute(
                            ExecutionRequest.of(bid, "ds", false), sample().iterator());
                    assertEquals(4, r.total());
                }
            }));
        }
        futures.add(pool.submit(() -> {
            for (int i = 0; i < 200; i++) {
                for (String bid : List.of("b1", "b2", "c-0-0")) {
                    BatchLookup lookup = svc.lookup(bid);
                    if (lookup instanceof BatchLookup.Found found) {
                        // 读到的必须是完整结论
                        assertEquals(found.result().total(),
                                found.result().violations().size());
                    }
                }
            }
        }));
        futures.add(pool.submit(() -> {
            for (int i = 0; i < 50; i++) {
                List<DataRecord> q = svc.exportQuarantined("b1");
                ReplayOutcome o = svc.replay("b1", q.iterator());
                assertEquals(3, o.batch().quarantinedCount(),
                        "verbatim export replay must keep b1 verdict stable");
            }
        }));
        for (var f : futures) {
            f.get(60, java.util.concurrent.TimeUnit.SECONDS);
        }
        pool.shutdown();
    }
}
