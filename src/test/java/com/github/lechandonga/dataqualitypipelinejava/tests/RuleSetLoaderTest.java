package com.github.lechandonga.dataqualitypipelinejava.tests;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.rule.InvalidRuleException;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetLoader;
import com.github.lechandonga.dataqualitypipelinejava.rule.RuleSetManager;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 声明式 JSON 加载与发布联动。
 */
class RuleSetLoaderTest {

    private static final String VALID = """
            [
              { "id": "name-not-null", "type": "NOT_NULL", "field": "name" },
              { "id": "age-range", "type": "RANGE", "field": "age",
                "params": { "min": "0", "max": "150" }, "nullable": true },
              { "id": "email-unique", "type": "UNIQUE", "field": "email" },
              { "id": "either-id", "type": "COMPOSITE", "field": "id",
                "params": { "mode": "ANY", "rules": "name-not-null,age-range" } }
            ]
            """;

    @Test
    void parsesAllRuleKinds() {
        List<RuleDefinition> defs = RuleSetLoader.parse(new StringReader(VALID));
        assertEquals(4, defs.size());
        RuleDefinition range = defs.get(1);
        assertEquals("0", range.params().get("min"));
        assertTrue(range.nullable());
        assertEquals(RuleType.COMPOSITE, defs.get(3).type());
        assertEquals("name-not-null,age-range", defs.get(3).params().get("rules"));

        RuleSetManager manager = new RuleSetManager();
        assertNotNull(manager.publish(defs).version());
    }

    @Test
    void malformedJsonRejected() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> RuleSetLoader.parse(new StringReader("[ { broken")));
        assertEquals("RULESET_PARSE_ERROR", ex.reasonCode());
    }

    @Test
    void topLevelMustBeArray() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> RuleSetLoader.parse(new StringReader("{ \"id\": \"x\" }")));
        assertEquals("RULESET_PARSE_ERROR", ex.reasonCode());
    }

    @Test
    void unknownTypeRejected() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> RuleSetLoader.parse(new StringReader(
                        "[{ \"id\": \"r\", \"type\": \"WEIRD\", \"field\": \"f\" }]")));
        assertEquals("RULE_TYPE_MISSING", ex.reasonCode());
    }

    @Test
    void invalidDefinitionInFileRejectsWholeSet() {
        String bad = """
                [
                  { "id": "good", "type": "NOT_NULL", "field": "a" },
                  { "id": "bad", "type": "RANGE", "field": "b",
                    "params": { "min": "9", "max": "1" } }
                ]
                """;
        List<RuleDefinition> defs = RuleSetLoader.parse(new StringReader(bad));
        RuleSetManager manager = new RuleSetManager();
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> manager.publish(defs));
        assertEquals("RANGE_INVALID", ex.reasonCode(),
                "文件中一条非法规则必须拒绝整个规则集，不静默跳过");
    }

    @Test
    void loadMissingFileGivesDistinctReason() {
        InvalidRuleException ex = assertThrows(InvalidRuleException.class,
                () -> RuleSetLoader.load(Path.of("/no/such/rules-12345.json")));
        assertEquals("RULESET_IO_ERROR", ex.reasonCode());
    }

    @Test
    void loadFromPathRoundTrip() throws Exception {
        Path file = Files.createTempFile("rules", ".json");
        Files.writeString(file, VALID);
        List<RuleDefinition> defs = RuleSetLoader.load(file);
        assertEquals(4, defs.size());
    }
}
