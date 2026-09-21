package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.config.PipelineProperties;
import com.github.lechandonga.dataqualitypipelinejava.engine.DefaultQualityPipeline;
import com.github.lechandonga.dataqualitypipelinejava.engine.InMemoryQuarantineService;
import com.github.lechandonga.dataqualitypipelinejava.engine.PipelineException;
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
 * 多字段基线必须整体生效或完全不生效：第二个字段提交失败时，
 * 第一个字段通过回滚保持提交前的值，不允许出现半更新基线。
 */
class MultiFieldCommitAtomicityTest {

    /** 第一个字段正常、第二个字段 commit 必失败的存储。 */
    static class PartialFailingStore extends InMemoryBaselineStore {
        boolean failOnCommit;

        PartialFailingStore(boolean failOnCommit) {
            this.failOnCommit = failOnCommit;
        }

        @Override
        public void prepareCommit(Baseline next) {
            // 预检阶段不失败，模拟“真正写入时才暴露的故障”
        }

        @Override
        public void commit(Baseline next) {
            if (failOnCommit && "b".equals(next.field())) {
                throw new PipelineException("BASELINE_IO_ERROR", "字段 b 磁盘故障");
            }
            super.commit(next);
        }
    }

    private PipelineProperties props() {
        PipelineProperties p = new PipelineProperties();
        p.setBaselineFields("a,b");
        p.setBaselineStore("memory");
        return p;
    }

    private RuleSetManager rules() {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(
                new RuleDefinition("nn", RuleType.NOT_NULL, "name", Map.of(), false)));
        return manager;
    }

    @Test
    void failureOnSecondFieldRollsBackFirstField() {
        PartialFailingStore store = new PartialFailingStore(false);
        DefaultQualityPipeline setup = new DefaultQualityPipeline(props(), rules(),
                store, new InMemoryQuarantineService());
        // 先建立两个字段的历史基线
        setup.execute("seed", IntStream.range(0, 6).mapToObj(i ->
                Map.of("id", String.valueOf(i), "name", "n",
                        "a", "10", "b", "20")));
        Baseline aBefore = store.load("a").orElseThrow();
        Baseline bBefore = store.load("b").orElseThrow();

        // 打开故障后执行新批次
        store.failOnCommit = true;
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(props(), rules(),
                store, new InMemoryQuarantineService());
        BatchResult result = pipeline.execute("doomed", IntStream.range(0, 6).mapToObj(i ->
                Map.of("id", "x" + i, "name", "n",
                        "a", "999", "b", "999")));

        assertEquals(BatchStatus.FAILED, result.status());
        assertEquals("BASELINE_COMMIT_FAILED", result.rejectReason());
        assertEquals(aBefore, store.load("a").orElseThrow(),
                "第一个字段必须回滚到提交前，不允许半更新");
        assertEquals(bBefore, store.load("b").orElseThrow(),
                "第二个字段保持原值");
    }

    @Test
    void prepareFailureBlocksAllWrites() {
        InMemoryBaselineStore store = new InMemoryBaselineStore() {
            @Override
            public void prepareCommit(Baseline next) {
                if ("b".equals(next.field())) {
                    throw new PipelineException("BASELINE_IO_ERROR", "预检失败");
                }
            }
        };
        DefaultQualityPipeline pipeline = new DefaultQualityPipeline(props(), rules(),
                store, new InMemoryQuarantineService());
        BatchResult result = pipeline.execute("blocked", IntStream.range(0, 6).mapToObj(i ->
                Map.of("id", String.valueOf(i), "name", "n",
                        "a", "10", "b", "20")));
        assertEquals(BatchStatus.FAILED, result.status());
        assertTrue(store.load("a").isEmpty(), "预检失败时任何字段都不得写入");
        assertTrue(store.load("b").isEmpty());
    }
}
