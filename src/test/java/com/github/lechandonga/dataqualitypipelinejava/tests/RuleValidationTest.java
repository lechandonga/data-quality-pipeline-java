package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 规则非法必须被拒绝，且原因码可区分；发布失败时旧规则集保持可用。
 */
class RuleValidationTest {

    private InvalidRuleException publish(RuleDefinition... defs) {
        RuleSetManager manager = new RuleSetManager();
        return assertThrows(InvalidRuleException.class,
                () -> manager.publish(List.of(defs)));
    }

    @Test
    void missingId() {
        InvalidRuleException ex = publish(
                new RuleDefinition("  ", RuleType.NOT_NULL, "f", Map.of(), false));
        assertEquals("RULE_ID_MISSING", ex.reasonCode());
    }

    @Test
    void missingType() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", null, "f", Map.of(), false));
        assertEquals("RULE_TYPE_MISSING", ex.reasonCode());
    }

    @Test
    void missingField() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.NOT_NULL, "", Map.of(), false));
        assertEquals("FIELD_MISSING", ex.reasonCode());
    }

    @Test
    void rangeMissingBothBounds() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.RANGE, "age", Map.of(), false));
        assertEquals("PARAM_MISSING", ex.reasonCode());
    }

    @Test
    void rangeMinNotNumber() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.RANGE, "age",
                        Map.of("min", "abc", "max", "10"), false));
        assertEquals("PARAM_INVALID", ex.reasonCode());
    }

    @Test
    void rangeMinGreaterThanMax() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.RANGE, "age",
                        Map.of("min", "10", "max", "1"), false));
        assertEquals("RANGE_INVALID", ex.reasonCode());
    }

    @Test
    void regexMissingPattern() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.REGEX, "email", Map.of(), false));
        assertEquals("PARAM_MISSING", ex.reasonCode());
    }

    @Test
    void regexInvalidPattern() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.REGEX, "email",
                        Map.of("pattern", "[unclosed"), false));
        assertEquals("PATTERN_INVALID", ex.reasonCode());
    }

    @Test
    void crossFieldMissingOtherField() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.CROSS_FIELD, "start",
                        Map.of("operator", "EQUALS"), false));
        assertEquals("PARAM_MISSING", ex.reasonCode());
    }

    @Test
    void crossFieldUnknownOperator() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.CROSS_FIELD, "start",
                        Map.of("otherField", "end", "operator", "SAME"), false));
        assertEquals("PARAM_INVALID", ex.reasonCode());
    }

    @Test
    void notNullNullableConflict() {
        InvalidRuleException ex = publish(
                new RuleDefinition("r1", RuleType.NOT_NULL, "f", Map.of(), true));
        assertEquals("NULLABLE_CONFLICT", ex.reasonCode());
    }

    @Test
    void duplicateIdsRejected() {
        InvalidRuleException ex = publish(
                new RuleDefinition("dup", RuleType.NOT_NULL, "a", Map.of(), false),
                new RuleDefinition("dup", RuleType.NOT_NULL, "b", Map.of(), false));
        assertEquals("RULE_ID_DUPLICATE", ex.reasonCode());
    }

    @Test
    void compositeBadMode() {
        InvalidRuleException ex = publish(
                new RuleDefinition("c1", RuleType.COMPOSITE, "a",
                        Map.of("mode", "SOME", "rules", "r1"), false));
        assertEquals("COMPOSITE_INVALID", ex.reasonCode());
    }

    @Test
    void compositeMissingReference() {
        InvalidRuleException ex = publish(
                new RuleDefinition("c1", RuleType.COMPOSITE, "a",
                        Map.of("mode", "ALL", "rules", "ghost"), false));
        assertEquals("COMPOSITE_REF_MISSING", ex.reasonCode());
    }

    @Test
    void compositeCycleRejected() {
        RuleDefinition a = new RuleDefinition("a", RuleType.COMPOSITE, "f",
                Map.of("mode", "ALL", "rules", "b"), false);
        RuleDefinition b = new RuleDefinition("b", RuleType.COMPOSITE, "f",
                Map.of("mode", "ALL", "rules", "a"), false);
        InvalidRuleException ex = publish(a, b);
        assertEquals("COMPOSITE_CYCLE", ex.reasonCode());
    }

    @Test
    void failedPublishKeepsPreviousSnapshot() {
        RuleSetManager manager = new RuleSetManager();
        manager.publish(List.of(
                new RuleDefinition("good", RuleType.NOT_NULL, "f", Map.of(), false)));
        assertEquals(1, manager.current().orderedRules().size());

        assertThrows(InvalidRuleException.class, () -> manager.publish(List.of(
                new RuleDefinition("bad", RuleType.RANGE, "f", Map.of(), false))));

        // 发布失败：旧版本完全不受影响，不出现半更新
        assertNotNull(manager.current());
        assertEquals("good", manager.current().orderedRules().get(0).id());
    }
}
