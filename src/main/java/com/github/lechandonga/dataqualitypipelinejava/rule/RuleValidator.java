package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 规则定义合法性校验。
 *
 * <p>非法定义一律抛出 {@link InvalidRuleException}，携带可区分的原因码，
 * 调用方（{@link RuleSetManager}）必须拒绝整个规则集，不得静默跳过。</p>
 *
 * <p>原因码约定：</p>
 * <ul>
 *   <li>RULE_ID_MISSING / RULE_ID_DUPLICATE：规则标识缺失或重复</li>
 *   <li>RULE_TYPE_MISSING：规则类型为空</li>
 *   <li>FIELD_MISSING：主字段未声明</li>
 *   <li>PARAM_MISSING：必需参数缺失</li>
 *   <li>PARAM_INVALID：参数取值非法（数值不可解析、运算符未知等）</li>
 *   <li>PATTERN_INVALID：正则无法编译</li>
 *   <li>RANGE_INVALID：min/max 关系错误</li>
 *   <li>NULLABLE_CONFLICT：NOT_NULL 规则同时声明 nullable=true</li>
 *   <li>COMPOSITE_INVALID：组合规则结构非法（mode 未知、子规则列表为空等）</li>
 * </ul>
 */
public final class RuleValidator {

    static final Set<String> CROSS_FIELD_OPERATORS = Set.of(
            "EQUALS", "NOT_EQUALS",
            "GREATER_THAN", "LESS_THAN",
            "GREATER_EQUAL", "LESS_EQUAL");

    private RuleValidator() {
    }

    public static void validate(RuleDefinition definition) {
        if (definition == null) {
            throw new InvalidRuleException("RULE_DEF_MISSING", "规则定义不能为空");
        }
        String id = trim(definition.id());
        if (id == null) {
            throw new InvalidRuleException("RULE_ID_MISSING", "规则 id 不能为空");
        }
        RuleType type = definition.type();
        if (type == null) {
            throw new InvalidRuleException("RULE_TYPE_MISSING",
                    "规则 " + id + " 的 type 不能为空");
        }
        if (trim(definition.field()) == null) {
            throw new InvalidRuleException("FIELD_MISSING",
                    "规则 " + id + " 必须声明 field");
        }
        if (type == RuleType.NOT_NULL && definition.nullable()) {
            throw new InvalidRuleException("NULLABLE_CONFLICT",
                    "规则 " + id + " 为 NOT_NULL 却声明 nullable=true，语义冲突");
        }
        Map<String, String> params = definition.params() == null
                ? Map.of() : definition.params();
        switch (type) {
            case NOT_NULL -> {
                // 无额外参数
            }
            case UNIQUE -> {
                // scope 参数可选，缺省为批次内 GLOBAL
            }
            case RANGE -> validateRange(id, params);
            case REGEX -> validateRegex(id, params);
            case CROSS_FIELD -> validateCrossField(id, params);
            case COMPOSITE -> validateComposite(id, params);
        }
    }

    private static void validateComposite(String id, Map<String, String> params) {
        String mode = trim(params.get("mode"));
        if (mode == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "组合规则 " + id + " 必须提供 mode(ALL|ANY)");
        }
        if (!"ALL".equals(mode) && !"ANY".equals(mode)) {
            throw new InvalidRuleException("COMPOSITE_INVALID",
                    "组合规则 " + id + " 的 mode 只能是 ALL 或 ANY，实际: " + mode);
        }
        String rules = trim(params.get("rules"));
        if (rules == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "组合规则 " + id + " 必须提供 rules(逗号分隔的子规则 id)");
        }
        String[] refs = rules.split(",");
        if (refs.length == 0) {
            throw new InvalidRuleException("COMPOSITE_INVALID",
                    "组合规则 " + id + " 的子规则列表为空");
        }
        for (String ref : refs) {
            if (trim(ref) == null) {
                throw new InvalidRuleException("COMPOSITE_INVALID",
                        "组合规则 " + id + " 的子规则 id 存在空白项");
            }
            if (ref.trim().equals(id)) {
                throw new InvalidRuleException("COMPOSITE_INVALID",
                        "组合规则 " + id + " 不能直接引用自身");
            }
        }
    }

    private static void validateRange(String id, Map<String, String> params) {
        String min = trim(params.get("min"));
        String max = trim(params.get("max"));
        if (min == null && max == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "规则 " + id + " (RANGE) 必须至少提供 min 或 max");
        }
        double minValue = Double.NEGATIVE_INFINITY;
        double maxValue = Double.POSITIVE_INFINITY;
        if (min != null) {
            minValue = parseDoubleOrThrow(id, "min", min);
        }
        if (max != null) {
            maxValue = parseDoubleOrThrow(id, "max", max);
        }
        if (minValue > maxValue) {
            throw new InvalidRuleException("RANGE_INVALID",
                    "规则 " + id + " 的 min(" + min + ") 大于 max(" + max + ")");
        }
    }

    private static double parseDoubleOrThrow(String id, String param, String raw) {
        try {
            double v = Double.parseDouble(raw);
            if (Double.isNaN(v) || Double.isInfinite(v)) {
                throw new NumberFormatException("not finite");
            }
            return v;
        } catch (NumberFormatException e) {
            throw new InvalidRuleException("PARAM_INVALID",
                    "规则 " + id + " 的 " + param + " 不是有限数值: " + raw);
        }
    }

    private static void validateRegex(String id, Map<String, String> params) {
        String pattern = trim(params.get("pattern"));
        if (pattern == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "规则 " + id + " (REGEX) 必须提供 pattern");
        }
        try {
            Pattern.compile(pattern);
        } catch (PatternSyntaxException e) {
            throw new InvalidRuleException("PATTERN_INVALID",
                    "规则 " + id + " 的正则无法编译: " + e.getDescription());
        }
    }

    private static void validateCrossField(String id, Map<String, String> params) {
        String otherField = trim(params.get("otherField"));
        if (otherField == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "规则 " + id + " (CROSS_FIELD) 必须提供 otherField");
        }
        String operator = trim(params.get("operator"));
        if (operator == null) {
            throw new InvalidRuleException("PARAM_MISSING",
                    "规则 " + id + " (CROSS_FIELD) 必须提供 operator");
        }
        if (!CROSS_FIELD_OPERATORS.contains(operator)) {
            throw new InvalidRuleException("PARAM_INVALID",
                    "规则 " + id + " 的 operator 未知: " + operator
                            + "，允许值: " + CROSS_FIELD_OPERATORS);
        }
    }

    private static String trim(String value) {
        if (value == null) {
            return null;
        }
        String t = value.trim();
        return t.isEmpty() ? null : t;
    }
}
