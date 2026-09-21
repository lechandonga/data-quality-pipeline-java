package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;
import com.github.lechandonga.dataqualitypipelinejava.rule.CompiledRule;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 各规则类型在记录级别的求值语义。
 */
class RuleEvaluationTest {

    @Test
    void notNullFailsOnMissingAndBlank() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("n", RuleType.NOT_NULL, "name", Map.of(), false));
        assertFalse(rule.evaluate(Map.of()).isEmpty());
        assertTrue(rule.evaluate(Map.of("name", "alice")).isEmpty());
        assertTrue(rule.evaluate(Map.of("name", "   ")).stream()
                .anyMatch(f -> f.reason().contains("为空")));
    }

    @Test
    void rangeBoundaryValues() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("r", RuleType.RANGE, "age",
                        Map.of("min", "0", "max", "100"), false));
        // 边界值恰好通过
        assertTrue(rule.evaluate(Map.of("age", "0")).isEmpty());
        assertTrue(rule.evaluate(Map.of("age", "100")).isEmpty());
        // 越界失败
        ValidationFailure low = rule.evaluate(Map.of("age", "-0.01")).get(0);
        assertEquals("r", low.ruleId());
        assertEquals("age", low.field());
        assertTrue(low.reason().contains("小于下限"));
        assertTrue(rule.evaluate(Map.of("age", "100.01")).stream()
                .anyMatch(f -> f.reason().contains("大于上限")));
        // 非数值可区分原因
        assertTrue(rule.evaluate(Map.of("age", "xx")).stream()
                .anyMatch(f -> f.reason().contains("不是合法数值")));
    }

    @Test
    void rangeOneSided() {
        CompiledRule minOnly = CompiledRule.compile(
                new RuleDefinition("r", RuleType.RANGE, "age", Map.of("min", "18"), false));
        assertTrue(minOnly.evaluate(Map.of("age", "9999")).isEmpty());
        assertTrue(minOnly.evaluate(Map.of("age", "17")).stream()
                .anyMatch(f -> f.reason().contains("小于下限")));
    }

    @Test
    void regexSemantics() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("p", RuleType.REGEX, "code",
                        Map.of("pattern", "[A-Z]{2}\\d{3}"), false));
        assertTrue(rule.evaluate(Map.of("code", "AB123")).isEmpty());
        ValidationFailure f = rule.evaluate(Map.of("code", "ab123")).get(0);
        assertEquals("p", f.ruleId());
        assertEquals("code", f.field());
        assertTrue(f.reason().contains("不匹配格式"));
    }

    @Test
    void crossFieldEquals() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("x", RuleType.CROSS_FIELD, "a",
                        Map.of("otherField", "b", "operator", "EQUALS"), false));
        assertTrue(rule.evaluate(Map.of("a", "1", "b", "1")).isEmpty());
        assertTrue(rule.evaluate(Map.of("a", "1", "b", "2")).stream()
                .anyMatch(f -> f.ruleId().equals("x")));
        assertTrue(rule.evaluate(Map.of("a", "1")).stream()
                .anyMatch(f -> f.reason().contains("缺少输入")));
    }

    @Test
    void crossFieldNumericOrdering() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("x", RuleType.CROSS_FIELD, "end",
                        Map.of("otherField", "start", "operator", "GREATER_THAN"), false));
        assertTrue(rule.evaluate(Map.of("start", "1", "end", "2")).isEmpty());
        assertTrue(rule.evaluate(Map.of("start", "2", "end", "1")).stream()
                .anyMatch(f -> f.reason().contains("跨字段一致性冲突")));
        assertTrue(rule.evaluate(Map.of("start", "x", "end", "2")).stream()
                .anyMatch(f -> f.reason().contains("均为数值")));
    }

    @Test
    void nullableSkipsRule() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("r", RuleType.REGEX, "code",
                        Map.of("pattern", "\\d+"), true));
        assertTrue(rule.evaluate(Map.of()).isEmpty());
        assertTrue(rule.evaluate(Map.of("code", " ")).isEmpty());
        assertTrue(rule.evaluate(Map.of("code", "12")).isEmpty());
        assertFalse(rule.evaluate(Map.of("code", "xx")).isEmpty());
    }

    @Test
    void compositeAllAndAny() {
        RuleDefinition base1 = new RuleDefinition("r1", RuleType.REGEX, "a",
                Map.of("pattern", "\\d+"), false);
        RuleDefinition base2 = new RuleDefinition("r2", RuleType.RANGE, "a",
                Map.of("min", "0", "max", "9"), false);
        RuleDefinition allDef = new RuleDefinition("cAll", RuleType.COMPOSITE, "a",
                Map.of("mode", "ALL", "rules", "r1,r2"), false);
        RuleDefinition anyDef = new RuleDefinition("cAny", RuleType.COMPOSITE, "a",
                Map.of("mode", "ANY", "rules", "r1,r2"), false);

        CompiledRule all = CompiledRule.composite(allDef,
                List.of(CompiledRule.compile(base1), CompiledRule.compile(base2)));
        CompiledRule any = CompiledRule.composite(anyDef,
                List.of(CompiledRule.compile(base1), CompiledRule.compile(base2)));

        assertTrue(all.evaluate(Map.of("a", "5")).isEmpty());
        // 10 是数字但超范围：ALL 失败、ANY 成功
        assertFalse(all.evaluate(Map.of("a", "10")).isEmpty());
        assertTrue(any.evaluate(Map.of("a", "10")).isEmpty());
        // xx 两者都失败
        assertFalse(all.evaluate(Map.of("a", "xx")).isEmpty());
        assertFalse(any.evaluate(Map.of("a", "xx")).isEmpty());
    }

    @Test
    void failureCarriesRuleAndField() {
        CompiledRule rule = CompiledRule.compile(
                new RuleDefinition("r9", RuleType.NOT_NULL, "email", Map.of(), false));
        ValidationFailure f = rule.evaluate(Map.of("email", "")).get(0);
        assertEquals("r9", f.ruleId());
        assertEquals("NOT_NULL", f.ruleType());
        assertEquals("email", f.field());
        assertNotNull(f.reason());
    }
}
