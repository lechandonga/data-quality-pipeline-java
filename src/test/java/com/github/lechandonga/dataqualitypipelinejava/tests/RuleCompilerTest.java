package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.CompositeMode;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleCompiler;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSet;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.composite;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.cross;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.notNull;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.range;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.regex;
import static com.github.lechandonga.dataqualitypipelinejava.tests.TestData.unique;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 规则冲突与非法定义必须被拒绝，且原因码可区分。 */
class RuleCompilerTest {

    private final RuleCompiler compiler = new RuleCompiler();

    private RuleDefinition def(String id, RuleType type) {
        return new RuleDefinition(id, type, null, null, null, null,
                null, null, null, null, null);
    }

    @Test
    void rejectsMissingId() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(def(" ", RuleType.NOT_NULL))));
        assertEquals(InvalidRuleException.Reason.MISSING_ID, ex.reason());
    }

    @Test
    void rejectsDuplicateId() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(notNull("r1", "a"), notNull("r1", "b"))));
        assertEquals(InvalidRuleException.Reason.DUPLICATE_ID, ex.reason());
    }

    @Test
    void rejectsUnsupportedType() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(def("r1", null))));
        assertEquals(InvalidRuleException.Reason.UNSUPPORTED_TYPE, ex.reason());
    }

    @Test
    void rejectsMissingField() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(def("r1", RuleType.NOT_NULL))));
        assertEquals(InvalidRuleException.Reason.MISSING_FIELD, ex.reason());
    }

    @Test
    void rejectsReversedRange() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(range("r1", "age", "100", "0"))));
        assertEquals(InvalidRuleException.Reason.INVALID_RANGE, ex.reason());
    }

    @Test
    void rejectsNonNumericRangeBound() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(range("r1", "age", "abc", null))));
        assertEquals(InvalidRuleException.Reason.INVALID_RANGE, ex.reason());
    }

    @Test
    void rejectsRangeWithoutBounds() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        new RuleDefinition("r1", RuleType.RANGE, "age", null, null,
                                null, null, null, null, null, null))));
        assertEquals(InvalidRuleException.Reason.INVALID_RANGE, ex.reason());
    }

    @Test
    void rejectsInvalidRegex() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(regex("r1", "email", "[unclosed"))));
        assertEquals(InvalidRuleException.Reason.INVALID_REGEX, ex.reason());
    }

    @Test
    void rejectsBlankRegex() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(regex("r1", "email", "  "))));
        assertEquals(InvalidRuleException.Reason.INVALID_REGEX, ex.reason());
    }

    @Test
    void rejectsInvalidOperator() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(cross("r1", "a", "b", "WHATEVER"))));
        assertEquals(InvalidRuleException.Reason.INVALID_OPERATOR, ex.reason());
    }

    @Test
    void rejectsUnknownChild() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        composite("c1", CompositeMode.AND, List.of("missing")))));
        assertEquals(InvalidRuleException.Reason.UNKNOWN_CHILD, ex.reason());
    }

    @Test
    void rejectsSelfReferencingComposite() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        composite("c1", CompositeMode.AND, List.of("c1")))));
        assertEquals(InvalidRuleException.Reason.RECURSIVE_COMPOSITE, ex.reason());
    }

    @Test
    void rejectsMutuallyRecursiveComposite() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        composite("c1", CompositeMode.AND, List.of("c2")),
                        composite("c2", CompositeMode.AND, List.of("c1")))));
        assertEquals(InvalidRuleException.Reason.RECURSIVE_COMPOSITE, ex.reason());
    }

    @Test
    void rejectsCompositeWithoutChildren() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        composite("c1", CompositeMode.AND, List.of()))));
        assertEquals(InvalidRuleException.Reason.MISSING_CHILDREN, ex.reason());
    }

    @Test
    void rejectsUniqueWithoutFields() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(unique("u1", List.of()))));
        assertEquals(InvalidRuleException.Reason.MISSING_FIELD, ex.reason());
    }

    @Test
    void rejectsUniqueReferencedAsChild() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> compiler.compile(List.of(
                        unique("u1", List.of("id")),
                        composite("c1", CompositeMode.AND, List.of("u1")))));
        assertEquals(InvalidRuleException.Reason.UNKNOWN_CHILD, ex.reason());
    }

    @Test
    void compilesValidSetAndVersionIsStable() {
        RuleSet a = compiler.compile(List.of(notNull("r1", "a"), range("r2", "x", "0", "9")));
        RuleSet b = compiler.compile(List.of(notNull("r1", "a"), range("r2", "x", "0", "9")));
        RuleSet c = compiler.compile(List.of(notNull("r1", "a")));
        assertEquals(2, a.rowRules().size());
        assertEquals(0, a.uniqueRules().size());
        assertEquals(a.version(), b.version(), "same definitions must yield same version");
        assertNotEquals(a.version(), c.version());
    }

    @Test
    void uniqueRulesAreSeparatedFromRowRules() {
        RuleSet set = compiler.compile(List.of(
                notNull("r1", "a"), unique("u1", List.of("id"))));
        assertEquals(1, set.rowRules().size());
        assertEquals(1, set.uniqueRules().size());
    }

    @Test
    void compilesReusableComposite() {
        RuleSet set = compiler.compile(List.of(
                notNull("base", "a"),
                composite("c1", CompositeMode.OR, List.of("base")),
                composite("c2", CompositeMode.AND, List.of("base", "c1"))));
        assertEquals(3, set.rowRules().size());
        assertTrue(set.rowRule("c2") != null);
    }
}
