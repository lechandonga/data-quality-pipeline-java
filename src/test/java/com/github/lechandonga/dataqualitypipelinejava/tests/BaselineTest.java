package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.baseline.Baseline;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineChecker;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineMerge;
import com.github.lechandonga.dataqualitypipelinejava.baseline.BaselineStats;
import com.github.lechandonga.dataqualitypipelinejava.baseline.InMemoryBaselineStore;
import com.github.lechandonga.dataqualitypipelinejava.baseline.StatsAccumulator;
import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 基线缺失零误报、漂移检测、累积合并与提交保护。
 */
class BaselineTest {

    private BaselineStats statsOf(String... values) {
        StatsAccumulator acc = new StatsAccumulator();
        for (String v : values) {
            if (v == null) {
                acc.acceptNull();
            } else {
                acc.acceptValue(v);
            }
        }
        return acc.snapshot();
    }

    @Test
    void missingBaselineRaisesNoAlarm() {
        BaselineChecker checker = new BaselineChecker(3.0, 0.1);
        List<ValidationFailure> failures = checker.check("amount",
                statsOf("1", "2", "3"), Optional.empty());
        assertTrue(failures.isEmpty(), "首次运行基线缺失必须零告警");
    }

    @Test
    void nullBaselineOptionalTreatedAsMissing() {
        BaselineChecker checker = new BaselineChecker(3.0, 0.1);
        assertEquals(0, checker.check("f", statsOf("1"), null).size());
    }

    @Test
    void stableDistributionPasses() {
        BaselineChecker checker = new BaselineChecker(3.0, 0.1);
        BaselineStats historical = statsOf("10", "11", "9", "10", "10");
        BaselineStats current = statsOf("10", "10", "11", "9", "10");
        assertTrue(checker.check("amount", current, Optional.of(historical)).isEmpty());
    }

    @Test
    void meanShiftDetected() {
        BaselineChecker checker = new BaselineChecker(2.0, 0.9);
        BaselineStats historical = statsOf("10", "10", "10", "10", "12", "8");
        BaselineStats current = statsOf("50", "51", "49", "50", "50", "50");
        List<ValidationFailure> failures = checker.check("amount", current,
                Optional.of(historical));
        assertTrue(failures.stream().anyMatch(f -> f.ruleId().equals("BASELINE_MEAN_SHIFT")));
        assertEquals("amount", failures.get(0).field());
    }

    @Test
    void nullRatioSpikeDetected() {
        BaselineChecker checker = new BaselineChecker(100.0, 0.1);
        BaselineStats historical = statsOf("1", "1", "1", "1", "1", "1", "1", "1", "1", "1");
        BaselineStats current = statsOf(
                null, null, null, null, null, "1", "1", "1", "1", "1");
        List<ValidationFailure> failures = checker.check("amount", current,
                Optional.of(historical));
        assertTrue(failures.stream().anyMatch(f -> f.ruleId().equals("BASELINE_NULL_RATIO")));
    }

    @Test
    void smallSampleSuppressesDrift() {
        BaselineChecker checker = new BaselineChecker(0.1, 0.0, 5L);
        BaselineStats historical = statsOf("10");
        BaselineStats current = statsOf("100");
        assertTrue(checker.check("amount", current, Optional.of(historical)).isEmpty(),
                "样本不足时不得告警");
    }

    @Test
    void constantDistributionBreakDetected() {
        BaselineChecker checker = new BaselineChecker(3.0, 0.9);
        BaselineStats historical = statsOf("5", "5", "5", "5", "5");
        BaselineStats current = statsOf("6", "6", "6", "6", "6");
        List<ValidationFailure> failures = checker.check("amount", current,
                Optional.of(historical));
        assertTrue(failures.stream().anyMatch(f -> f.ruleId().equals("BASELINE_CONSTANT_BREAK")));
    }

    @Test
    void firstCommitCreatesBaseline() {
        BaselineStats current = statsOf("1", "2", "3");
        Baseline baseline = BaselineMerge.merge("amount", Optional.empty(), current);
        assertEquals(1, baseline.batchCount());
        assertEquals(3, baseline.totalCount());
        assertEquals(2.0, baseline.mean(), 1e-9);
    }

    @Test
    void mergeAccumulatesBatches() {
        BaselineStats first = statsOf("0", "2");
        Baseline b1 = BaselineMerge.merge("amount", Optional.empty(), first);
        BaselineStats second = statsOf("0", "2");
        Baseline b2 = BaselineMerge.merge("amount", Optional.of(b1), second);
        assertEquals(2, b2.batchCount());
        assertEquals(4, b2.totalCount());
        assertEquals(1.0, b2.mean(), 1e-9);
    }

    @Test
    void accumulatorOrderIndependent() {
        StatsAccumulator a = new StatsAccumulator();
        StatsAccumulator b = new StatsAccumulator();
        List<String> values = List.of("1", "5", "8", "12", "30", "42", "7", "9");
        List<String> reverse = new java.util.ArrayList<>(values);
        java.util.Collections.reverse(reverse);
        for (String v : values) {
            a.acceptValue(v);
        }
        for (String v : reverse) {
            b.acceptValue(v);
        }
        BaselineStats sa = a.snapshot();
        BaselineStats sb = b.snapshot();
        assertEquals(sa.mean(), sb.mean(), 1e-12);
        assertEquals(sa.stdDev(), sb.stdDev(), 1e-12);
        assertEquals(sa.count(), sb.count());
    }

    @Test
    void storeCommitIsReferenceAtomic() {
        InMemoryBaselineStore store = new InMemoryBaselineStore();
        Baseline b = new Baseline("f", 1, 10, 9, 9, 1.0, 0.0, 9);
        store.commit(b);
        assertEquals(b, store.load("f").orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> store.commit(null));
        assertEquals(b, store.load("f").orElseThrow(), "非法提交不得改变已有基线");
    }

    @Test
    void invalidCheckerConfigRejected() {
        assertThrows(IllegalArgumentException.class, () -> new BaselineChecker(0, 0.1));
        assertThrows(IllegalArgumentException.class, () -> new BaselineChecker(3, -1));
    }
}
