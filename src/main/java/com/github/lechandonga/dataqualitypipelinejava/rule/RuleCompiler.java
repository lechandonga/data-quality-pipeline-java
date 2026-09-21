package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

/**
 * 将声明式 {@link RuleDefinition} 列表编译为不可变 {@link RuleSet}。
 * 任一规则非法即整体拒绝（fail-fast），绝不产出半成品规则集，也不得静默跳过；
 * 每种非法情形对应 {@link InvalidRuleException.Reason} 中可区分的原因码。
 */
@Component
public class RuleCompiler {

    private static final Set<String> OPERATORS = Set.of("EQ", "NE", "GT", "GE", "LT", "LE");

    public RuleSet compile(List<RuleDefinition> definitions) {
        List<RuleDefinition> defs = definitions == null ? List.of() : List.copyOf(definitions);

        // 第一遍：结构校验 + 建立 id 索引（重复 id 在此拒绝）
        Map<String, RuleDefinition> byId = new LinkedHashMap<>();
        for (RuleDefinition def : defs) {
            validateShape(def);
            if (byId.putIfAbsent(def.id(), def) != null) {
                throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.DUPLICATE_ID,
                        "rule id duplicated: " + def.id());
            }
        }

        // 第二遍：类型相关校验（范围/正则/算子），仍不依赖规则间引用
        for (RuleDefinition def : defs) {
            validateParameters(def);
        }

        // 第三遍：解析组合引用（未知子规则、递归组合在此拒绝）
        Map<String, RowRule> resolved = new LinkedHashMap<>();
        List<RowRule> rowRules = new ArrayList<>();
        List<UniqueRule> uniqueRules = new ArrayList<>();
        for (RuleDefinition def : defs) {
            if (def.type() == RuleType.UNIQUE) {
                // UNIQUE 由引擎跨记录单点判定，不作为 RowRule
                uniqueRules.add(new UniqueRule(def.id(), fieldsOf(def)));
            } else {
                resolve(def, byId, resolved, new ArrayList<>());
                // 以定义顺序输出（resolve 可能先解析了被引用的子规则）
                RowRule top = resolved.get(def.id());
                if (!rowRules.contains(top)) {
                    rowRules.add(top);
                }
            }
        }

        String version = "sha256:" + digest(defs);
        return new RuleSet(version, List.copyOf(rowRules), List.copyOf(uniqueRules));
    }

    private void validateShape(RuleDefinition def) {
        if (def == null) {
            throw new InvalidRuleException(null, InvalidRuleException.Reason.MISSING_ID,
                    "rule definition is null");
        }
        if (def.id() == null || def.id().isBlank()) {
            throw new InvalidRuleException(String.valueOf(def.id()),
                    InvalidRuleException.Reason.MISSING_ID, "rule id is required");
        }
        RuleType type = def.type();
        if (type == null) {
            throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.UNSUPPORTED_TYPE,
                    "rule type is required, supported: " + List.of(RuleType.values()));
        }
        switch (type) {
            case NOT_NULL, RANGE, REGEX -> requireField(def);
            case CROSS_FIELD -> {
                requireField(def);
                if (def.otherField() == null || def.otherField().isBlank()) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.MISSING_FIELD,
                            "cross-field rule requires otherField");
                }
            }
            case UNIQUE -> {
                List<String> f = fieldsOf(def);
                if (f.isEmpty()) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.MISSING_FIELD,
                            "unique rule requires at least one field");
                }
            }
            case COMPOSITE -> {
                if (def.children() == null || def.children().isEmpty()) {
                    throw new InvalidRuleException(def.id(),
                            InvalidRuleException.Reason.MISSING_CHILDREN,
                            "composite rule requires non-empty children");
                }
            }
        }
    }

    private void validateParameters(RuleDefinition def) {
        switch (def.type()) {
            case RANGE -> {
                Double min;
                Double max;
                try {
                    min = Rules.parseBound(def.min());
                    max = Rules.parseBound(def.max());
                } catch (IllegalArgumentException e) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.INVALID_RANGE,
                            e.getMessage());
                }
                if (min == null && max == null) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.INVALID_RANGE,
                            "range rule requires at least one of min/max");
                }
                if (min != null && max != null && min > max) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.INVALID_RANGE,
                            "range min(" + min + ") must be <= max(" + max + ")");
                }
            }
            case REGEX -> {
                if (def.pattern() == null || def.pattern().isBlank()) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.INVALID_REGEX,
                            "regex rule requires pattern");
                }
                try {
                    Rules.compilePattern(def.pattern());
                } catch (PatternSyntaxException e) {
                    throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.INVALID_REGEX,
                            "invalid regex: " + e.getDescription());
                }
            }
            case CROSS_FIELD -> {
                if (def.operator() == null || !OPERATORS.contains(def.operator())) {
                    throw new InvalidRuleException(def.id(),
                            InvalidRuleException.Reason.INVALID_OPERATOR,
                            "operator must be one of " + OPERATORS);
                }
            }
            default -> {
                // 其他类型无额外参数校验
            }
        }
    }

    private RowRule resolve(RuleDefinition def, Map<String, RuleDefinition> byId,
                            Map<String, RowRule> resolved, List<String> stack) {
        RowRule cached = resolved.get(def.id());
        if (cached != null) {
            return cached;
        }
        if (stack.contains(def.id())) {
            throw new InvalidRuleException(def.id(),
                    InvalidRuleException.Reason.RECURSIVE_COMPOSITE,
                    "recursive composite chain: " + stack + " -> " + def.id());
        }
        RowRule rule = switch (def.type()) {
            case NOT_NULL -> new Rules.NotNullRule(def.id(), def.field());
            case RANGE -> new Rules.RangeRule(def.id(), def.field(),
                    Rules.parseBound(def.min()), Rules.parseBound(def.max()));
            case REGEX -> new Rules.RegexRule(def.id(), def.field(),
                    Rules.compilePattern(def.pattern()));
            case CROSS_FIELD -> new Rules.CrossFieldRule(def.id(), def.field(),
                    def.otherField(), def.operator());
            case UNIQUE -> throw new InvalidRuleException(def.id(),
                    InvalidRuleException.Reason.UNKNOWN_CHILD,
                    "unique rule cannot be referenced as child rule");
            case COMPOSITE -> {
                stack.add(def.id());
                List<RowRule> children = new ArrayList<>();
                for (String childId : def.children()) {
                    RuleDefinition childDef = byId.get(childId);
                    if (childDef == null) {
                        throw new InvalidRuleException(def.id(),
                                InvalidRuleException.Reason.UNKNOWN_CHILD,
                                "child rule not found: " + childId);
                    }
                    children.add(resolve(childDef, byId, resolved, stack));
                }
                stack.remove(stack.size() - 1);
                yield new Rules.CompositeRule(def.id(), def.mode(), children);
            }
        };
        resolved.put(def.id(), rule);
        return rule;
    }

    private void requireField(RuleDefinition def) {
        if (def.field() == null || def.field().isBlank()) {
            throw new InvalidRuleException(def.id(), InvalidRuleException.Reason.MISSING_FIELD,
                    "rule requires target field");
        }
    }

    private List<String> fieldsOf(RuleDefinition def) {
        if (def.children() == null) {
            return List.of();
        }
        List<String> f = new ArrayList<>();
        for (String c : def.children()) {
            if (c != null && !c.isBlank()) {
                f.add(c);
            }
        }
        return List.copyOf(f);
    }

    private String digest(List<RuleDefinition> defs) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (RuleDefinition def : defs) {
                md.update(def.toString().getBytes(StandardCharsets.UTF_8));
                md.update((byte) '\n');
            }
            return HexFormat.of().formatHex(md.digest()).substring(0, 16);
        } catch (Exception e) {
            throw new IllegalStateException("sha-256 unavailable", e);
        }
    }
}
