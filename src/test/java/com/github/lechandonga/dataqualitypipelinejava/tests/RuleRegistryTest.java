package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleRegistry;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RuleRegistryTest {

    @Test
    void failedReplacementKeepsOldVersionUntouched() {
        RuleRegistry registry = new RuleRegistry(new RuleCompiler());
        RuleSet v1 = registry.replaceAll(List.of(notNull("r1", "a")));

        assertThrows(InvalidRuleException.class,
                () -> registry.replaceAll(List.of(range("bad", "x", "10", "0"))));

        RuleSet after = registry.current();
        assertSame(v1, after, "invalid update must have no effect at all");
        assertEquals(v1.version(), after.version());
    }

    @Test
    void inFlightBatchSnapshotIsUnaffectedByLaterUpdate() {
        RuleRegistry registry = new RuleRegistry(new RuleCompiler());
        RuleSet v1 = registry.replaceAll(List.of(notNull("r1", "a")));
        // 模拟"进行中批次持有快照引用"
        RuleSet bound = registry.current();
        registry.replaceAll(List.of(notNull("r1", "a"), notNull("r2", "b")));

        assertEquals(1, bound.rowRules().size(), "bound snapshot must be immutable");
        assertSame(v1, bound);
        assertEquals(2, registry.current().rowRules().size());
    }

    @Test
    void emptyRulesBeforeAnyPublishAreDeterministic() {
        RuleRegistry registry = new RuleRegistry(new RuleCompiler());
        assertEquals(0, registry.current().rowRules().size());
    }
}
