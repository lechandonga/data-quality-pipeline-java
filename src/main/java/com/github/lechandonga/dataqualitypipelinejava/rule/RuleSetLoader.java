package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 声明式规则集 JSON 加载器。
 *
 * <p>JSON 结构示例：</p>
 * <pre>{@code
 * [
 *   { "id": "name-not-null", "type": "NOT_NULL", "field": "name" },
 *   { "id": "age-range", "type": "RANGE", "field": "age",
 *     "params": { "min": "0", "max": "150" }, "nullable": true },
 *   { "id": "email-format", "type": "REGEX", "field": "email",
 *     "params": { "pattern": "^[^@]+@[^@]+$" } },
 *   { "id": "end-after-start", "type": "CROSS_FIELD", "field": "end",
 *     "params": { "otherField": "start", "operator": "GREATER_THAN" } },
 *   { "id": "either-id", "type": "COMPOSITE", "field": "id",
 *     "params": { "mode": "ANY", "rules": "id-format,legacy-id-format" } },
 *   { "id": "email-unique", "type": "UNIQUE", "field": "email" }
 * ]
 * }</pre>
 *
 * <p>加载只做解析；合法性仍统一由 {@link RuleValidator}/{@link RuleSetManager}
 * 在发布时校验，非法规则导致整个规则集发布失败，不会静默跳过。</p>
 */
public final class RuleSetLoader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RuleSetLoader() {
    }

    public static List<RuleDefinition> load(Path path) {
        try (Reader reader = Files.newBufferedReader(path)) {
            return parse(reader);
        } catch (IOException e) {
            throw new InvalidRuleException("RULESET_IO_ERROR",
                    "读取规则文件失败: " + path + " - " + e.getMessage());
        }
    }

    public static List<RuleDefinition> load(InputStream in) {
        try {
            return parse(MAPPER.reader().readTree(in));
        } catch (IOException e) {
            throw new InvalidRuleException("RULESET_PARSE_ERROR",
                    "规则集 JSON 解析失败: " + e.getMessage());
        }
    }

    public static List<RuleDefinition> parse(Reader reader) {
        try {
            return parse(MAPPER.readTree(reader));
        } catch (IOException e) {
            throw new InvalidRuleException("RULESET_PARSE_ERROR",
                    "规则集 JSON 解析失败: " + e.getMessage());
        }
    }

    private static List<RuleDefinition> parse(com.fasterxml.jackson.databind.JsonNode root) {
        if (root == null || !root.isArray()) {
            throw new InvalidRuleException("RULESET_PARSE_ERROR",
                    "规则集顶层必须是数组");
        }
        List<RuleDefinition> definitions = new ArrayList<>();
        for (com.fasterxml.jackson.databind.JsonNode node : root) {
            definitions.add(toDefinition(node));
        }
        return definitions;
    }

    private static RuleDefinition toDefinition(com.fasterxml.jackson.databind.JsonNode node) {
        String id = text(node, "id");
        String typeRaw = text(node, "type");
        RuleType type;
        try {
            type = RuleType.valueOf(typeRaw);
        } catch (IllegalArgumentException e) {
            throw new InvalidRuleException("RULE_TYPE_MISSING",
                    "未知或缺失的规则类型: " + typeRaw + "（规则 id=" + id + "）");
        }
        String field = text(node, "field");
        boolean nullable = node.has("nullable") && node.get("nullable").asBoolean(false);
        Map<String, String> params = new HashMap<>();
        com.fasterxml.jackson.databind.JsonNode paramsNode = node.get("params");
        if (paramsNode != null && paramsNode.isObject()) {
            paramsNode.fields().forEachRemaining(entry ->
                    params.put(entry.getKey(), entry.getValue().asText()));
        }
        return new RuleDefinition(id, type, field, Map.copyOf(params), nullable);
    }

    private static String text(com.fasterxml.jackson.databind.JsonNode node, String key) {
        com.fasterxml.jackson.databind.JsonNode value = node.get(key);
        return value == null || value.isNull() ? null : value.asText();
    }
}
