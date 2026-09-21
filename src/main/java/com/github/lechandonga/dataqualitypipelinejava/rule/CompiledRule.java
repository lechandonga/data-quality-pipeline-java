package com.github.lechandonga.dataqualitypipelinejava.rule;

import com.github.lechandonga.dataqualitypipelinejava.model.RuleDefinition;
import com.github.lechandonga.dataqualitypipelinejava.model.RuleType;
import com.github.lechandonga.dataqualitypipelinejava.model.ValidationFailure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 编译后的不可变规则，对单条记录求值。
 *
 * <p>每个实例对应一条 {@link RuleDefinition}；COMPOSITE 规则持有已编译的子规则，
 * 支持递归组合复用。求值纯函数化：同一记录必然得到同一结论，保证可复现。</p>
 *
 * <p>UNIQUE 为跨记录规则，{@link #evaluate(Map)} 返回空，唯一性由引擎借助
 * {@link UniqueTracker} 在批次维度统一判定。</p>
 */
public final class CompiledRule {

    private final RuleDefinition definition;
    private final List<CompiledRule> children;
    private final Pattern compiledPattern;

    private CompiledRule(RuleDefinition definition,
                         List<CompiledRule> children,
                         Pattern compiledPattern) {
        this.definition = definition;
        this.children = children;
        this.compiledPattern = compiledPattern;
    }

    /**
     * 编译叶子规则（非 COMPOSITE）。
     */
    public static CompiledRule compile(RuleDefinition definition) {
        RuleValidator.validate(definition);
        Pattern pattern = null;
        if (definition.type() == RuleType.REGEX) {
            try {
                pattern = Pattern.compile(definition.params().get("pattern").trim());
            } catch (PatternSyntaxException e) {
                throw new InvalidRuleException("PATTERN_INVALID",
                        "规则 " + definition.id() + " 的正则无法编译: " + e.getDescription());
            }
        }
        return new CompiledRule(definition, List.of(), pattern);
    }

    /**
     * 编译组合规则，子规则必须已经编译，且引用关系已通过结构校验。
     */
    public static CompiledRule composite(RuleDefinition definition,
                                         List<CompiledRule> children) {
        RuleValidator.validate(definition);
        if (definition.type() != RuleType.COMPOSITE) {
            throw new InvalidRuleException("COMPOSITE_INVALID",
                    "规则 " + definition.id() + " 不是 COMPOSITE 类型");
        }
        if (children.isEmpty()) {
            throw new InvalidRuleException("COMPOSITE_INVALID",
                    "组合规则 " + definition.id() + " 没有任何已解析的子规则");
        }
        return new CompiledRule(definition, List.copyOf(children), null);
    }

    public RuleDefinition definition() {
        return definition;
    }

    public String id() {
        return definition.id();
    }

    public RuleType type() {
        return definition.type();
    }

    public String field() {
        return definition.field();
    }

    /**
     * 对单条记录求值；无违规则返回空列表。
     */
    public List<ValidationFailure> evaluate(Map<String, String> record) {
        if (definition.type() == RuleType.COMPOSITE) {
            return evaluateComposite(record);
        }
        if (definition.type() == RuleType.UNIQUE) {
            return List.of();
        }
        String value = record.get(definition.field());
        boolean isNull = value == null || value.trim().isEmpty();
        if (isNull && definition.nullable()) {
            return List.of();
        }
        return switch (definition.type()) {
            case NOT_NULL -> isNull
                    ? List.of(failure(definition.field(), "字段为空"))
                    : List.of();
            case RANGE -> evaluateRange(value);
            case REGEX -> evaluateRegex(value, isNull);
            case CROSS_FIELD -> evaluateCrossField(record, value);
            default -> List.of();
        };
    }

    private List<ValidationFailure> evaluateRange(String value) {
        if (value == null) {
            return List.of(failure(definition.field(), "字段为空，无法进行范围校验"));
        }
        double numeric;
        try {
            numeric = Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return List.of(failure(definition.field(), "不是合法数值: " + value));
        }
        String minRaw = definition.params().get("min");
        String maxRaw = definition.params().get("max");
        if (minRaw != null && numeric < Double.parseDouble(minRaw.trim())) {
            return List.of(failure(definition.field(),
                    "数值 " + numeric + " 小于下限 " + minRaw.trim()));
        }
        if (maxRaw != null && numeric > Double.parseDouble(maxRaw.trim())) {
            return List.of(failure(definition.field(),
                    "数值 " + numeric + " 大于上限 " + maxRaw.trim()));
        }
        return List.of();
    }

    private List<ValidationFailure> evaluateRegex(String value, boolean isNull) {
        if (isNull) {
            return List.of(failure(definition.field(), "字段为空，无法进行格式校验"));
        }
        if (!compiledPattern.matcher(value).matches()) {
            return List.of(failure(definition.field(),
                    "值不匹配格式 " + definition.params().get("pattern").trim()));
        }
        return List.of();
    }

    private List<ValidationFailure> evaluateCrossField(Map<String, String> record, String value) {
        String otherField = definition.params().get("otherField").trim();
        String operator = definition.params().get("operator").trim();
        String otherValue = record.get(otherField);
        if (value == null || otherValue == null) {
            return List.of(failure(definition.field(),
                    "跨字段校验缺少输入: " + definition.field()
                            + "=" + value + ", " + otherField + "=" + otherValue));
        }
        boolean violated;
        String expect;
        switch (operator) {
            case "EQUALS" -> {
                violated = !value.equals(otherValue);
                expect = "应等于";
            }
            case "NOT_EQUALS" -> {
                violated = value.equals(otherValue);
                expect = "应不等于";
            }
            default -> {
                double left;
                double right;
                try {
                    left = Double.parseDouble(value.trim());
                    right = Double.parseDouble(otherValue.trim());
                } catch (NumberFormatException e) {
                    return List.of(failure(definition.field(),
                            "跨字段比较需要双方均为数值: " + value + ", " + otherValue));
                }
                violated = switch (operator) {
                    case "GREATER_THAN" -> !(left > right);
                    case "LESS_THAN" -> !(left < right);
                    case "GREATER_EQUAL" -> !(left >= right);
                    case "LESS_EQUAL" -> !(left <= right);
                    default -> true;
                };
                expect = switch (operator) {
                    case "GREATER_THAN" -> "应大于";
                    case "LESS_THAN" -> "应小于";
                    case "GREATER_EQUAL" -> "应大于等于";
                    default -> "应小于等于";
                };
            }
        }
        if (violated) {
            return List.of(failure(definition.field(),
                    "跨字段一致性冲突: " + definition.field() + "=" + value
                            + " 对 " + otherField + "=" + otherValue + " " + expect));
        }
        return List.of();
    }

    private List<ValidationFailure> evaluateComposite(Map<String, String> record) {
        boolean all = "ALL".equals(definition.params().get("mode").trim());
        List<ValidationFailure> collected = new ArrayList<>();
        for (CompiledRule child : children) {
            List<ValidationFailure> childFailures = child.evaluate(record);
            if (childFailures.isEmpty()) {
                if (!all) {
                    // ANY：任一子规则通过即整体通过
                    return List.of();
                }
            } else {
                if (all) {
                    collected.addAll(childFailures);
                }
            }
        }
        if (all) {
            return collected;
        }
        // ANY 且全部失败：给出组合规则自身的失败说明
        return List.of(failure(definition.field(),
                "组合规则(ANY) 的所有子规则均未通过: " + definition.params().get("rules").trim()));
    }

    private ValidationFailure failure(String field, String reason) {
        return new ValidationFailure(definition.id(),
                definition.type().name(), field, reason);
    }
}
