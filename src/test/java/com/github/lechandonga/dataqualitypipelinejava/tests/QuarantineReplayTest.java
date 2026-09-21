package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.BatchRejectedException;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.engine.QuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchResult;
import com.github.lechandonga.dataqualitypipelinejava.model.BatchStatus;
import com.github.lechandonga.dataqualitypipelinejava.model.RecordResult;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 失败记录隔离与修复后重放：
 * 原因可定位规则/字段、重放不重复计数、已通过记录结论不变。
 */
class QuarantineReplayTest {

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        p.setMaxRecordsPerBatch(1000);
        return p;
    }

    private DefaultQualityPipeline pipeline(RuleSetManager manager,
                                            InMemoryQuarantineService quarantine) {
        return new DefaultQualityPipeline(props(), manager,
                new InMemoryBaselineStore(), quarantine);
    }

    private RuleSetManager managerWith(RuleDefinition... defs) {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(defs));
        return manager;
    }

    @Test
    void failedRecordsQuarantinedWithRuleFieldReasons() {
        InMemoryQuarantineService q = new InMemoryQuarantineService();
        RuleSetManager manager = managerWith(
                new RuleDefinition("nn", RuleType.NOT_NULL, "email", Map.of(), false),
                new RuleDefinition("rx", RuleType.REGEX, "code",
                        Map.of("pattern", "\\d{3}"), false));
        DefaultQualityPipeline pipeline = pipeline(manager, q);

        BatchResult result = pipeline.execute("b", Stream.of(
                Map.of("id", "1", "email", "a@x", "code", "123"),
                Map.of("id", "2", "code", "123"),          // email 缺失
                Map.of("id", "3", "email", "c@x", "code", "xx"))); // code 格式错

        assertEquals(BatchStatus.COMMITTED, result.status());
        assertEquals(2, q.size("b"));
        RecordResult bad = q.list("b").stream()
                .filter(r -> r.recordKey().equals("3")).findFirst().orElseThrow();
        assertEquals("rx", bad.failures().get(0).ruleId());
        assertEquals("REGEX", bad.failures().get(0).ruleType());
        assertEquals("code", bad.failures().get(0).field());
    }

    @Test
    void replayFixedRecordPassesWithoutDoubleCounting() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("nn", RuleType.NOT_NULL, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        BatchResult first = pipeline.execute("b", Stream.of(
                Map.of("id", "1", "email", "a@x"),
                Map.of("id", "2")));
        assertEquals(1, first.passedCount());
        assertEquals(1, first.failedCount());
        assertEquals(1, first.cumulativePassed());

        BatchResult replay = pipeline.replay("b", List.of(
                new QuarantineService.ReplayInput("2", Map.of("id", "2", "email", "b@x"))));

        assertEquals(1, replay.passedCount(), "本次重放通过 1 条");
        assertEquals(0, replay.failedCount());
        assertEquals(2, replay.cumulativePassed(), "累计通过不重复计数");
        assertEquals(BatchStatus.COMMITTED, replay.status());
    }

    @Test
    void replayStillFailingKeepsRecordQuarantined() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("nn", RuleType.NOT_NULL, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        pipeline.execute("b", Stream.of(Map.of("id", "1"), Map.of("id", "2")));

        BatchResult replay = pipeline.replay("b", List.of(
                new QuarantineService.ReplayInput("1", Map.of("id", "1")), // 仍缺 email
                new QuarantineService.ReplayInput("2", Map.of("id", "2", "email", "z@z"))));

        assertEquals(1, replay.passedCount());
        assertEquals(1, replay.failedCount());
        assertEquals(BatchStatus.ISOLATED, replay.status());
        assertEquals(1, replay.rejectedRecords().size());
        assertEquals("1", replay.rejectedRecords().get(0).recordKey());
        assertEquals("nn", replay.rejectedRecords().get(0).failures().get(0).ruleId());
    }

    @Test
    void replayOfAlreadyPassedRecordDoesNotChangeConclusion() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("nn", RuleType.NOT_NULL, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        BatchResult first = pipeline.execute("b", Stream.of(
                Map.of("id", "1", "email", "a@x"),
                Map.of("id", "2")));
        BatchResult replay = pipeline.replay("b", List.of(
                // 已通过的 1 号再次提交；不存在的 9 号也混入
                new QuarantineService.ReplayInput("1", Map.of("id", "1")),
                new QuarantineService.ReplayInput("9", Map.of("id", "9")),
                new QuarantineService.ReplayInput("2", Map.of("id", "2", "email", "b@x"))));

        assertEquals(1, replay.passedCount(), "只有原本隔离的 2 号被计数");
        assertEquals(2, replay.cumulativePassed());
        assertEquals(first.passedCount(), 1);
    }

    @Test
    void replayRecomputesUniqueWithinCurrentQuarantineSet() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("u", RuleType.UNIQUE, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        pipeline.execute("b", Stream.of(
                Map.of("id", "1", "email", "dup"),
                Map.of("id", "2", "email", "dup"),
                Map.of("id", "3", "email", "unique3")));
        // 1 号先出现通过；2 号因重复隔离
        BatchResult beforeReplay = pipeline.replay("b", List.of());
        assertEquals(1, beforeReplay.rejectedRecords().size());

        // 把 2 号修复成全新值：应通过；改成 1 号的已占用值仍会失败（但 1 号不在隔离区，
        // 重放语义只在“当前仍隔离的集合”内重判，因此新值通过）
        BatchResult fixed = pipeline.replay("b", List.of(
                new QuarantineService.ReplayInput("2",
                        Map.of("id", "2", "email", "fixed@x"))));
        assertEquals(1, fixed.passedCount());
        assertEquals(0, fixed.failedCount());
        assertEquals(BatchStatus.COMMITTED, fixed.status());
    }

    @Test
    void replayIdempotentWhenRecordStillFailing() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("nn", RuleType.NOT_NULL, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        pipeline.execute("b", Stream.of(Map.of("id", "1")));
        var badInput = new QuarantineService.ReplayInput("1", Map.of("id", "1"));

        BatchResult r1 = pipeline.replay("b", List.of(badInput));
        BatchResult r2 = pipeline.replay("b", List.of(badInput));
        assertEquals(1, r1.failedCount());
        assertEquals(1, r2.failedCount(), "同一失败记录重复重放不会累积计数");
        assertEquals(0, r2.cumulativePassed());
    }

    @Test
    void replayUnknownBatchRejected() {
        DefaultQualityPipeline pipeline = pipeline(
                managerWith(new RuleDefinition("nn", RuleType.NOT_NULL, "f",
                        Map.of(), false)),
                new InMemoryQuarantineService());
        BatchRejectedException ex = assertThrows(BatchRejectedException.class,
                () -> pipeline.replay("ghost", List.of()));
        assertEquals("BATCH_NOT_FOUND", ex.reasonCode());
    }

    @Test
    void replayUsesBatchOriginalRuleSnapshotAfterUpdate() {
        RuleSetManager manager = managerWith(
                new RuleDefinition("v1nn", RuleType.NOT_NULL, "email", Map.of(), false));
        DefaultQualityPipeline pipeline = pipeline(manager, new InMemoryQuarantineService());

        pipeline.execute("b", Stream.of(Map.of("id", "1"))); // email 缺失，隔离

        // 规则演进到 v2，但重放必须仍按 v1 的结论维度判定
        manager.publish(List.of(
                new RuleDefinition("v2name", RuleType.NOT_NULL, "name", Map.of(), false)));

        BatchResult replay = pipeline.replay("b", List.of(
                // 满足旧 v1（email 有值），但不满足新 v2（name 缺失）；按 v1 应通过
                new QuarantineService.ReplayInput("1",
                        Map.of("id", "1", "email", "fixed@x"))));
        assertEquals(1, replay.passedCount());
        assertEquals("v1", replay.ruleSetVersion(), "重放锁定批次当时的规则版本");
    }

    @Test
    void quarantineListIsSortedDeterministically() {
        InMemoryQuarantineService q = new InMemoryQuarantineService();
        q.put("b", new RecordResult("z", false, List.of()));
        q.put("b", new RecordResult("a", false, List.of()));
        q.put("b", new RecordResult("m", false, List.of()));
        assertEquals(List.of("a", "m", "z"),
                q.list("b").stream().map(RecordResult::recordKey).toList());
    }
}
